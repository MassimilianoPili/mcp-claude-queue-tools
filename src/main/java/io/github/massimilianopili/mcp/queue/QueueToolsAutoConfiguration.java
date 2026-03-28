package io.github.massimilianopili.mcp.queue;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Import;

@AutoConfiguration
@ConditionalOnProperty(name = "mcp.taskqueue.enabled", havingValue = "true", matchIfMissing = false)
@Import({QueueConfig.class, ClaudeTaskQueueTools.class, IngestQueueRepository.class})
public class QueueToolsAutoConfiguration {
}
