# Build your own Java RAG AI Agent

 ⬅ This is the next workshop step after the [step-1](../workshop-step-1).

## Code, moar code, MOAR CODE

 🤩 The step builds the beginning of a functioning AI Agent by introducing
- DPR – Dense Passage Retrieval
- RAG – Retrival Augmented Generation
- VSS – Vector Similarity Search
- Parsing unstructured text
- Chunking strategies


 ♻️ And introduces the following technologies and techniques
- Spring AI's Vector Stores
- Apache Cassandra's Secondary Indexes and Vector data type
- Apache Tika to parse unstructured documents into text


This step introduces a new Decorating AI Agent `AiAgentVector` that adds the RAG capabilities, with the use of Spring AI's `VectorStore` interface.

The prompt template `system-prompt-qa.txt` adds some text to tell the LLM about the results from the Vector Similarity Search (VSS).

 📑 To upload documents (test or PDF files) go to the `http:localhost:8080/upload` url.  Any unstructured text in files, e.g. PDFs, will be parsed to plain text by Apache Tika.  Text is chunked into 300 words with 150 word overlaps.

The implementation of `VectorStore` used is `CassandraVectorStore`.  Its schema is created manually before startup.  This can be configured to use a different and/or existing table, if you so desire.  In real use-cases it will be expected to have multiple vector stores in different domains and on different data, hence its flexibility.

The default schema looks like…
```
CREATE TABLE datastax_ai_agent.vector_store (
    id text PRIMARY KEY,
    content text,
    embedding vector<float, 1536>
);

CREATE CUSTOM INDEX vector_store_embedding_idx
   ON datastax_ai_agent.vector_store (embedding)
   USING 'StorageAttachedIndex';
```
 ℹ️ The vector dimensions is automatic to the dimensions used by the embedding model you have configured in Spring AI.

 🧐 You might have noticed the `AiUploadUI` class is in the `.vector.` package and wondered why UI and Agents are in the same package.  This codebase is packaging-by-feature instead of packaging-by-layer.  This is an valuable approach that anyone that has worked refactoring large legacy codebases in the past may be familiar with.  More info [here](http://www.javapractices.com/topic/TopicAction.do?Id=205).

 🔎 To see changes this step introduces use `git diff workshop-step-1..workshop-step-2`.

## Manual schema setup

Schema initialization is disabled by default. Run [schema/step-2.cql](schema/step-2.cql) in the CQL console before starting this step. Run each earlier step's script in order when starting from a fresh keyspace. The keyspace is `datastax_ai_agent`; create it in the Astra console, or use the local Cassandra keyspace statement from step 1. The `ALTER TABLE` scripts run once per database.

The scripts use 1536-dimensional embeddings, matching the default OpenAI embedding model. Adjust the vector dimensions before creating the schema if you select a different embedding model.

## Configure and Build


 🏃🏿 Run the project like:
```
./mvnw clean spring-boot:run
```




 👩‍💻 Open in a browser http://localhost:8080
 and ask your chatbot some questions that requires specific information you know it doesn't have.


 👩‍💻 Open in a browser http://localhost:8080/upload
and upload a text or PDF file that contains that specific information.

Ask the questions testing for answers that deliver the new information.


 🔍 Explore the data that's been created in AstraDB.
- Open the AstraDB console, go to the `CQL Console`
- Type the command cql commands
```
USE datastax_ai_agent ;
DESCRIBE table vector_store ;
SELECT id FROM vector_store ;
SELECT id,content FROM vector_store ;
SELECT id,content,embedding FROM vector_store ;
```

## Tests

Run `./mvnw verify` to compile, test, and package the application. Tests use local model substitutes and do not need provider credentials.

The database tests require a Docker-compatible container runtime. They start Cassandra 5, apply the manual schema, and test native protocol V4 with schema initialization disabled. They check memory persistence and reject startup DDL. Vector steps also test insertion, search, and deletion.

## Next…

 💪🏽 To move on to [step-3](../workshop-step-3), do the following:
```
git switch workshop-step-3
```



***
![java](./src/assets/java.png) ![vaadin](./src/assets/vaadin.png) ![spring](./src/assets/spring.png) ![tika](./src/assets/tika.jpeg) ![openai](./src/assets/openai.png) ![cassandra](./src/assets/cassandra.png) ![tavily](./src/assets/tavily.jpeg)

***
All work is copyrighted to DataStax, Inc