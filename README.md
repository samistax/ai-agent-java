# Build your own Java RAG AI Agent

 ⬅ This is the next workshop step after the [step-0](../workshop-step-0).

## Code, moar code, MOAR CODE

 🤩 The step introduces a further basic concept of an AI Agent
- Short Term Memory

This is also know as Conversational Memory.

 ♻️ This step introduces the following technologies and techniques
- Apache Cassandra and/or AstraDB for persistence
- Spring AI's `.chat.memory.` packages
- Spring Boot Autoconfigure's Cassandra package


This step introduces a new Decorating AI Agent `AiAgentSession` that adds conversation history capabilities.

The prompt adds some text to tell the LLM about the history being added.

The history is stored and received through Spring AI's `ChatMemory` interface.  Here we use the `MessageWindowChatMemory` implementation on top of the `CassandraChatMemoryRepository` from Spring AI.

The `CassandraChatMemoryRepository` can create its own schema, but not on AstraDB.  Its `CREATE TYPE` statement carries a per-request keyspace, and the driver only permits that on native protocol V5, while AstraDB caps at V4.  So `spring.ai.chat.memory.repository.cassandra.initialize-schema` is set to `false` in `application.properties` and you create the two schema objects yourself.

 ⚠️ Create the schema before you run the project.  Open the AstraDB console, go to the `CQL Console`, and type the following.  The names are fixed by Spring AI and must match exactly.
```
CREATE TYPE IF NOT EXISTS datastax_ai_agent.ai_chat_message (
    msg_timestamp timestamp,
    msg_type      text,
    msg_content   text
);

CREATE TABLE IF NOT EXISTS datastax_ai_agent.agent_conversations (
    session_id        text,
    message_timestamp timestamp,
    messages          frozen<list<frozen<ai_chat_message>>>,
    PRIMARY KEY (session_id, message_timestamp)
  ) WITH CLUSTERING ORDER BY (message_timestamp DESC);
```

Running against a local Apache Cassandra instead of AstraDB?  Create the keyspace first, then the two objects above.
```
CREATE KEYSPACE IF NOT EXISTS datastax_ai_agent
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
```

The repository can be configured to use a different and/or existing table if you so desire.

 🔎 To see changes this step introduces use `git diff workshop-step-0..workshop-step-1`.

## Configure and Build


 🏃🏿 Run the project like:
```
./mvnw clean spring-boot:run
```





## Ask some questions…

 👩‍💻 Once fixed and running, open in a browser http://localhost:8080
 and ask your chatbot some questions.

Test the agents ability to remember the conversation.  The conversation is bound to the Vaadin session, so it will work over different browser tabs and limited periods of time as well.

The Agent is still limited though.  While is keeps memory of its interactions with you, it's only pulling knowledge its model has been trained on.

 🔍 Explore and test where these limitations are.

 🔍 Explore the data that's been created in AstraDB.
- Open the AstraDB console, go to the `CQL Console`
- Type the command cql commands
```
USE datastax_ai_agent ;
DESCRIBE agent_conversations ;
SELECT * FROM agent_conversations ;
```


## Next… 

 💪🏽 To move on to [step-2](../workshop-step-2) do the following:
```
git switch workshop-step-2
```



***
![java](./src/assets/java.png) ![vaadin](./src/assets/vaadin.png) ![spring](./src/assets/spring.png) ![tika](./src/assets/tika.jpeg) ![openai](./src/assets/openai.png) ![cassandra](./src/assets/cassandra.png) ![tavily](./src/assets/tavily.jpeg)

*** 
All work is copyrighted to DataStax, Inc
