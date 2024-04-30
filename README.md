# Build your own Java RAG AI Agent

 ⬅ This is the next workshop step after the [step-6](../workshop-step-6).

## Code, moar code, MOAR CODE

 🤩 This step adds the concept
- Vectorising time windows over time-series data

♻️ And introduces the techniques
- metadata and multiple indexes


 👩‍💻  …

 ℹ️ The chat-memory now stores its messages in a UDT list column, so the cache row itself keeps the assistant's answer in the added `assistant` column, written once the streamed response has completed.


 🔎 To see changes this step introduces use `git diff workshop-step-6..workshop-step-7`.

## Manual schema setup

Schema initialization is disabled by default. Run [schema/step-7.cql](schema/step-7.cql) in the CQL console before starting this step. Run each earlier step's script in order when starting from a fresh keyspace. The keyspace is `datastax_ai_agent`; create it in the Astra console, or use the local Cassandra keyspace statement from step 1. The `ALTER TABLE` scripts run once per database.

The scripts use 1536-dimensional embeddings, matching the default OpenAI embedding model. Adjust the vector dimensions before creating the schema if you select a different embedding model.

The conversation vector store and FCC vector setup honor `spring.ai.vectorstore.cassandra.initialize-schema`. Keep it `false` for a schema-disabled deployment.

## Build


 🏃🏿 Run the project like:
```
./mvnw clean spring-boot:run
```


## Ask some questions…

 👩‍💻 Open in a browser http://localhost:8080/import
 and enter the full path to your curr_datausage.csv file

Watch the console for progress on csv rows being imported.


## Tests

Run `./mvnw verify` to compile, test, and package the application. Tests use local model substitutes and do not need provider credentials.

The database tests require a Docker-compatible container runtime. They start Cassandra 5, apply the manual schema, and test native protocol V4 with schema initialization disabled. They check memory persistence and reject startup DDL. Vector steps also test insertion, search, and deletion.

## Next…

 💪🏽 The complete code repository for this workshop can be found at http://github.com/datastax/ai-agent-java




***
![java](./src/assets/java.png) ![vaadin](./src/assets/vaadin.png) ![spring](./src/assets/spring.png) ![tika](./src/assets/tika.jpeg) ![openai](./src/assets/openai.png) ![cassandra](./src/assets/cassandra.png) ![tavily](./src/assets/tavily.jpeg)

***
All work is copyrighted to DataStax, Inc