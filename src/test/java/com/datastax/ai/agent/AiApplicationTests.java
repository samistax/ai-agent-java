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
package com.datastax.ai.agent;

import java.util.List;
import java.util.Map;
import com.datastax.ai.agent.base.AiAgentBase;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import reactor.core.publisher.Flux;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = "spring.ai.openai.api-key=unused-local-test-key")
class AiApplicationTests {
    @Autowired @Qualifier("agent") AiAgentBase agent;
    @MockBean OpenAiChatModel model;

    @Test
    void buildsPromptAndStreamsModelResponse() {
        Prompt prompt = agent.createPrompt(new UserMessage("Hello"), Map.of(), OpenAiChatOptions.builder());
        when(model.stream(prompt)).thenReturn(Flux.just(
                new ChatResponse(List.of(new Generation(new AssistantMessage("Hello back"))))));
        assertThat(prompt.getContents()).contains("Hello");
        assertThat(agent.send(prompt).blockLast().getResult().getOutput().getText()).isEqualTo("Hello back");
        verify(model).stream(prompt);
    }
}
