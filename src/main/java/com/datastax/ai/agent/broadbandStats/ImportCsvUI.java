/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 */
package com.datastax.ai.agent.broadbandStats;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.ImportProgress;
import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.ImportResult;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.UI;
import com.vaadin.flow.component.UIDetachedException;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.details.Details;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.H3;
import com.vaadin.flow.component.html.Paragraph;
import com.vaadin.flow.component.html.Pre;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.progressbar.ProgressBar;
import com.vaadin.flow.component.upload.Upload;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.server.streams.TemporaryFileUploadHandler;
import com.vaadin.flow.server.streams.UploadMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Imports a CSV file of network-traffic samples in three steps: choose the file, start the import,
 * and watch it run. The uploaded file is kept in a temporary file, so files of hundreds of MB
 * are not held in memory, and the temporary file is deleted when the import is done.
 */
@PageTitle("Import broadband data")
@Route("import")
class ImportCsvUI extends VerticalLayout {

    private static final Logger logger = LoggerFactory.getLogger(ImportCsvUI.class);

    private static final long MAX_FILE_BYTES = 10_000_000L;

    private static final String SAMPLE_FILE = """
            unit_id,dtime,sk_tx_bytes,sk_rx_bytes,cust_wired_tx_bytes,cust_wired_rx_bytes,cust_wifi_tx_bytes,cust_wifi_rx_bytes
            641,2023-07-01 05:46:44,438883,4788487,591265,828814,6290547,309446146
            641,2023-07-01 06:46:46,873591,9274550,1353529,6884055,8966339,107103226""";

    // one import at a time: it writes to the shared tables and calls the embeddings API
    private static final AtomicBoolean IMPORT_RUNNING = new AtomicBoolean();
    private static final ExecutorService IMPORT_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "csv-import");
        thread.setDaemon(true);
        return thread;
    });

    private final FccBroadbandDataService service;

    // step 1 and 2
    private final Div steps = new Div();
    private final Upload upload;
    private final Span uploadError = new Span();
    private final Span readyText = new Span("Choose a file first.");
    private final Button startButton = new Button("Start import", VaadinIcon.PLAY.create());

    // while importing
    private final Div progressCard = card();
    private final Span progressTitle = new Span();
    private final ProgressBar progressBar = new ProgressBar();
    private final Span progressDetail = new Span();
    private final Button cancelButton = new Button("Cancel", VaadinIcon.CLOSE_SMALL.create());

    // when done
    private final Div resultCard = card();

    // the vaadin UI, as the upload handler and the import run outside of the UI's request thread
    private volatile UI ui;
    private volatile Path uploadedFile;
    private volatile String uploadedName;
    private volatile long uploadedBytes;
    private volatile boolean cancelled;

    public ImportCsvUI(FccBroadbandDataService fccBroadbandDataService) {
        this.service = fccBroadbandDataService;

        setWidthFull();
        setMaxWidth("760px");
        setPadding(true);
        setSpacing(true);
        getStyle().set("margin-inline", "auto");

        var handler = new TemporaryFileUploadHandler(this::onUploaded) {
            // enforced on the server, the limits of the upload component itself only guide the browser
            @Override
            public long getFileSizeMax() {
                return MAX_FILE_BYTES;
            }

            @Override
            public long getRequestSizeMax() {
                return MAX_FILE_BYTES + 1_000_000L;
            }

            @Override
            public long getFileCountMax() {
                return 1;
            }
        };
        upload = new Upload(handler);
        upload.setMaxFiles(1);
        upload.setMaxFileSize((int) MAX_FILE_BYTES);
        upload.setAcceptedFileTypes(".csv", "text/csv");
        upload.setUploadButton(new Button("Select CSV file…", VaadinIcon.UPLOAD.create()));
        upload.setDropLabel(new Span("Drop a CSV file here"));
        upload.setWidthFull();
        upload.addFileRejectedListener(e -> showUploadError(e.getErrorMessage()));
        upload.addFileRemovedListener(e -> {
            discardUploadedFile();
            clearUploadError();
            markNoFile();
        });

        add(header(), steps, progressCard, resultCard, formatHelp());

        buildSteps();
        buildProgressCard();
        progressCard.setVisible(false);
        resultCard.setVisible(false);

        addAttachListener(e -> ui = e.getUI());
        addDetachListener(e -> discardUploadedFile());
    }

    // ---------------------------------------------------------------- layout

    private Component header() {

        var back = new Button("Back to chat", VaadinIcon.ARROW_LEFT.create(), e -> UI.getCurrent().navigate(""));
        back.addThemeVariants(ButtonVariant.LUMO_TERTIARY);

        var title = new H2("Import broadband data");
        title.getStyle().set("margin", "0");

        var subtitle = new Paragraph(
                "Upload a CSV file of network-traffic samples. The rows are stored in Cassandra, "
                        + "and one embedding is created per unit and day, which lets the chat find days with a similar traffic pattern.");
        subtitle.getStyle().set("margin", "0").set("color", "var(--lumo-secondary-text-color)");

        var header = new VerticalLayout(back, title, subtitle);
        header.setPadding(false);
        header.setSpacing(false);
        header.getStyle().set("gap", "var(--lumo-space-s)");
        return header;
    }

    private void buildSteps() {

        // step 1: choose the file
        var chooseCard = card();
        uploadError.getStyle().set("color", "var(--lumo-error-text-color)").set("font-size", "var(--lumo-font-size-s)");
        uploadError.setVisible(false);
        uploadError.getElement().setAttribute("role", "alert");

        var limits = new Span("CSV files up to " + formatSize(MAX_FILE_BYTES) + ". Comma-separated, with a header line.");
        limits.getStyle().set("color", "var(--lumo-secondary-text-color)").set("font-size", "var(--lumo-font-size-s)");

        chooseCard.add(stepHeader(1, "Choose a file"), upload, uploadError, limits);

        // step 2: start
        var startCard = card();
        readyText.getStyle().set("color", "var(--lumo-secondary-text-color)");
        startButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        startButton.setEnabled(false);
        startButton.addClickListener(e -> startImport());

        var explanation = new Paragraph(
                "Importing the same file again replaces its rows instead of duplicating them. "
                        + "Creating the embeddings calls the OpenAI API, once for each unit and day in the file.");
        explanation.getStyle().set("margin", "0").set("color", "var(--lumo-secondary-text-color)").set("font-size", "var(--lumo-font-size-s)");

        var action = new HorizontalLayout(startButton, readyText);
        action.setAlignItems(FlexComponent.Alignment.CENTER);
        startCard.add(stepHeader(2, "Import"), explanation, action);

        steps.getStyle().set("display", "flex").set("flex-direction", "column").set("gap", "var(--lumo-space-m)").set("width", "100%");
        steps.add(chooseCard, startCard);
    }

    private void buildProgressCard() {

        progressTitle.getStyle().set("font-weight", "600");
        progressDetail.getStyle().set("color", "var(--lumo-secondary-text-color)").set("font-size", "var(--lumo-font-size-s)");
        progressBar.setWidthFull();
        progressBar.getElement().setAttribute("aria-label", "Import progress");

        cancelButton.addThemeVariants(ButtonVariant.LUMO_ERROR, ButtonVariant.LUMO_TERTIARY);
        cancelButton.addClickListener(e -> {
            cancelled = true;
            cancelButton.setEnabled(false);
            progressTitle.setText("Cancelling…");
        });

        var title = new H3("Importing");
        title.getStyle().set("margin", "0");

        var top = new HorizontalLayout(title, cancelButton);
        top.setWidthFull();
        top.setAlignItems(FlexComponent.Alignment.CENTER);
        top.setJustifyContentMode(FlexComponent.JustifyContentMode.BETWEEN);

        var body = new VerticalLayout(progressTitle, progressBar, progressDetail);
        body.setPadding(false);
        body.setSpacing(false);
        body.getStyle().set("gap", "var(--lumo-space-s)");

        progressCard.add(top, body);
    }

    private Component formatHelp() {

        var intro = new Paragraph(
                "Eight columns per line, with a header line. dtime is UTC and written as yyyy-MM-dd HH:mm:ss. "
                        + "The sizes are bytes sent (tx) and received (rx) during the sample's hour.");
        intro.getStyle().set("margin-top", "0");

        var sample = new Pre(SAMPLE_FILE);
        sample.getStyle()
                .set("margin", "0")
                .set("padding", "var(--lumo-space-m)")
                .set("overflow-x", "auto")
                .set("font-size", "var(--lumo-font-size-xs)")
                .set("background", "var(--lumo-contrast-5pct)")
                .set("border-radius", "var(--lumo-border-radius-m)");

        var details = new Details("Expected file format", new Div(intro, sample));
        details.setWidthFull();
        return details;
    }

    private static Div card() {
        var card = new Div();
        card.getStyle()
                .set("box-sizing", "border-box")
                .set("width", "100%")
                .set("padding", "var(--lumo-space-l)")
                .set("background", "var(--lumo-base-color)")
                .set("border", "1px solid var(--lumo-contrast-10pct)")
                .set("border-radius", "var(--lumo-border-radius-l)");
        return card;
    }

    private static Component stepHeader(int number, String text) {

        var badge = new Span(String.valueOf(number));
        badge.getStyle()
                .set("display", "inline-flex")
                .set("align-items", "center")
                .set("justify-content", "center")
                .set("width", "1.75rem")
                .set("height", "1.75rem")
                .set("flex-shrink", "0")
                .set("border-radius", "50%")
                .set("font-weight", "600")
                .set("font-size", "var(--lumo-font-size-s)")
                .set("color", "var(--lumo-primary-contrast-color)")
                .set("background", "var(--lumo-primary-color)");

        var title = new H3(text);
        title.getStyle().set("margin", "0");

        var header = new HorizontalLayout(badge, title);
        header.setAlignItems(FlexComponent.Alignment.CENTER);
        header.getStyle().set("margin-bottom", "var(--lumo-space-m)");
        return header;
    }

    // ---------------------------------------------------------------- upload

    /** Called by the upload handler, outside of the UI's request thread, once a file is fully received. */
    private void onUploaded(UploadMetadata metadata, File file) throws IOException {

        String name = metadata.fileName();
        UI target = ui;
        if (!name.toLowerCase(Locale.ROOT).endsWith(".csv")) {
            Files.deleteIfExists(file.toPath());
            access(target, () -> {
                upload.clearFileList();
                showUploadError("Only .csv files can be imported.");
            });
            return;
        }
        Path previous = uploadedFile;
        uploadedFile = file.toPath();
        uploadedName = name;
        uploadedBytes = file.length();
        if (null != previous) {
            Files.deleteIfExists(previous);
        }
        access(target, () -> {
            clearUploadError();
            readyText.setText(name + " (" + formatSize(uploadedBytes) + ") is ready.");
            startButton.setEnabled(true);
        });
    }

    private void discardUploadedFile() {
        Path file = uploadedFile;
        uploadedFile = null;
        if (null != file) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                logger.warn("Could not delete temporary upload {}", file, ex);
            }
        }
    }

    private void markNoFile() {
        startButton.setEnabled(false);
        readyText.setText("Choose a file first.");
    }

    private void showUploadError(String message) {
        uploadError.setText(null == message || message.isBlank() ? "The file was not accepted." : message);
        uploadError.setVisible(true);
    }

    private void clearUploadError() {
        uploadError.setVisible(false);
    }

    // ---------------------------------------------------------------- import

    private void startImport() {

        Path file = uploadedFile;
        if (null == file) {
            return;
        }
        if (!IMPORT_RUNNING.compareAndSet(false, true)) {
            showUploadError("Another import is running. Try again when it has finished.");
            return;
        }
        // from here on the import owns the temporary file and deletes it
        uploadedFile = null;
        cancelled = false;
        long size = Math.max(1, uploadedBytes);
        String name = uploadedName;

        clearUploadError();
        steps.setVisible(false);
        resultCard.setVisible(false);
        progressCard.setVisible(true);
        cancelButton.setEnabled(true);
        progressTitle.setText("Starting…");
        progressDetail.setText(name);
        progressBar.setIndeterminate(true);

        UI target = ui;
        IMPORT_EXECUTOR.execute(() -> runImport(target, file, name, size));
    }

    private void runImport(UI target, Path file, String name, long size) {

        ImportProgress progress = new ImportProgress() {

            @Override
            public void rows(long rows, long skipped, long bytesRead) {
                access(target, () -> {
                    progressTitle.setText("Storing rows and creating daily embeddings");
                    progressBar.setIndeterminate(false);
                    progressBar.setValue(Math.min(1.0, (double) bytesRead / size));
                    progressDetail.setText(format(rows) + " rows stored" + (0 < skipped ? ", " + format(skipped) + " lines skipped" : ""));
                });
            }

            @Override
            public void embeddings(int daysDone, int daysTotal) {
                access(target, () -> {
                    progressTitle.setText("Creating daily embeddings");
                    progressBar.setIndeterminate(false);
                    progressBar.setValue(0 < daysTotal ? (double) daysDone / daysTotal : 1.0);
                    progressDetail.setText(format(daysDone) + " of " + format(daysTotal) + " unit-days");
                });
            }

            @Override
            public boolean cancelled() {
                return cancelled;
            }
        };

        try {
            ImportResult result = service.importCsv(file, progress);
            access(target, () -> showResult(name, result));
        } catch (Exception ex) {
            // details go to the log only
            logger.error("CSV import of {} failed", name, ex);
            access(target, () -> showFailure(name));
        } finally {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ex) {
                logger.warn("Could not delete temporary upload {}", file, ex);
            }
            IMPORT_RUNNING.set(false);
        }
    }

    /** Runs in the UI, and does nothing if the user has already left the page. */
    private static void access(UI target, Runnable update) {
        if (null != target) {
            try {
                target.access(update::run);
            } catch (UIDetachedException ex) {
                logger.debug("The page was closed during the import");
            }
        }
    }

    // ---------------------------------------------------------------- result

    private void showResult(String name, ImportResult r) {

        progressCard.setVisible(false);
        resultCard.removeAll();

        boolean nothing = 0 == r.rows();
        String heading = r.cancelled() ? "Import cancelled"
                : nothing ? "Nothing was imported"
                : 0 < r.skipped() ? "Import finished, some lines were skipped"
                : "Import complete";
        String colour = r.cancelled() || nothing || 0 < r.skipped()
                ? "var(--lumo-warning-text-color)"
                : "var(--lumo-success-text-color)";

        var title = new H3(heading);
        title.getStyle().set("margin", "0").set("color", colour);
        title.getElement().setAttribute("role", "status");

        var file = new Span(name);
        file.getStyle().set("color", "var(--lumo-secondary-text-color)").set("font-size", "var(--lumo-font-size-s)");

        var tiles = new Div(
                tile("Rows imported", format(r.rows())),
                tile("Units", format(r.units())),
                tile("Days", format(r.days())),
                tile("Embeddings", format(r.embeddings())),
                tile("Lines skipped", format(r.skipped())));
        tiles.getStyle()
                .set("display", "grid")
                .set("grid-template-columns", "repeat(auto-fit, minmax(7.5rem, 1fr))")
                .set("gap", "var(--lumo-space-m)")
                .set("margin", "var(--lumo-space-m) 0");

        resultCard.add(title, file, tiles);

        if (r.cancelled()) {
            resultCard.add(note("The rows read so far were stored. Days that were read completely have their embedding, "
                    + "the day being read when you cancelled does not. "
                    + "Import the file again to finish, the stored rows are replaced, not duplicated."));
        } else if (nothing) {
            resultCard.add(note("No line could be read. Check that the file has the columns shown under \"Expected file format\"."));
        }
        if (0 < r.skipped() && null != r.firstError()) {
            resultCard.add(new Details("Why were lines skipped?", note(
                    "The first skipped line was " + r.firstError() + ". Each line needs eight comma-separated values, "
                            + "with dtime like 2023-07-01 05:46:44.")));
        }

        var again = new Button("Import another file", VaadinIcon.UPLOAD.create(), e -> reset());
        again.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        var chat = new Button("Back to chat", e -> UI.getCurrent().navigate(""));
        var actions = new HorizontalLayout(again, chat);
        actions.getStyle().set("margin-top", "var(--lumo-space-m)");
        resultCard.add(actions);

        resultCard.setVisible(true);
    }

    private void showFailure(String name) {

        progressCard.setVisible(false);
        resultCard.removeAll();

        var title = new H3("The import failed");
        title.getStyle().set("margin", "0").set("color", "var(--lumo-error-text-color)");
        title.getElement().setAttribute("role", "alert");

        var again = new Button("Try again", VaadinIcon.REFRESH.create(), e -> reset());
        again.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        again.getStyle().set("margin-top", "var(--lumo-space-m)");

        resultCard.add(title, note("The file " + name + " could not be imported. "
                + "Rows read before the problem were stored. Check the application log for the reason, "
                + "then import the file again."), again);
        resultCard.setVisible(true);
    }

    private void reset() {
        resultCard.setVisible(false);
        upload.clearFileList();
        clearUploadError();
        markNoFile();
        steps.setVisible(true);
    }

    private static Component tile(String label, String value) {

        var number = new Span(value);
        number.getStyle().set("font-size", "var(--lumo-font-size-xl)").set("font-weight", "600");

        var caption = new Span(label);
        caption.getStyle().set("font-size", "var(--lumo-font-size-xs)").set("color", "var(--lumo-secondary-text-color)");

        var tile = new Div(number, caption);
        tile.getStyle()
                .set("display", "flex")
                .set("flex-direction", "column")
                .set("padding", "var(--lumo-space-m)")
                .set("background", "var(--lumo-contrast-5pct)")
                .set("border-radius", "var(--lumo-border-radius-m)");
        return tile;
    }

    private static Paragraph note(String text) {
        var note = new Paragraph(text);
        note.getStyle().set("color", "var(--lumo-secondary-text-color)").set("font-size", "var(--lumo-font-size-s)");
        return note;
    }

    private static String format(long number) {
        return String.format(Locale.ROOT, "%,d", number);
    }

    private static String formatSize(long bytes) {
        if (bytes >= 1_000_000_000L) {
            return String.format(Locale.ROOT, "%.1f GB", bytes / 1e9);
        }
        if (bytes >= 1_000_000L) {
            return String.format(Locale.ROOT, "%.1f MB", bytes / 1e6);
        }
        return String.format(Locale.ROOT, "%.1f KB", bytes / 1e3);
    }
}
