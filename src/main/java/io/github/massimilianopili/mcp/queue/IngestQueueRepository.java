package io.github.massimilianopili.mcp.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Repository
public class IngestQueueRepository {

    private static final Logger log = LoggerFactory.getLogger(IngestQueueRepository.class);

    private final JdbcTemplate jdbc;

    public IngestQueueRepository(@Qualifier("taskQueueDataSource") javax.sql.DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    @PostConstruct
    public void init() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS ingest_queue (
                    id SERIAL PRIMARY KEY,
                    url TEXT NOT NULL,
                    extracted_json JSONB NOT NULL,
                    extract_type VARCHAR(50),
                    status VARCHAR(20) DEFAULT 'pending',
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    processed_at TIMESTAMP
                )
                """);
        jdbc.execute("""
                CREATE INDEX IF NOT EXISTS idx_ingest_queue_status
                ON ingest_queue(status) WHERE status = 'pending'
                """);
        log.info("IngestQueueRepository initialized (table ingest_queue ready)");
    }

    public void enqueue(String url, String extractedJson, String extractType) {
        try {
            jdbc.update(
                    "INSERT INTO ingest_queue(url, extracted_json, extract_type) VALUES(?, ?::jsonb, ?)",
                    url, extractedJson, extractType);
            log.debug("Enqueued ingest for: {}", url);
        } catch (Exception e) {
            log.warn("Failed to enqueue ingest for '{}': {}", url, e.getMessage());
        }
    }

    public List<Map<String, Object>> drainPending(int maxItems) {
        List<Map<String, Object>> items = jdbc.queryForList(
                "SELECT id, url, extracted_json::text as extracted_json, extract_type FROM ingest_queue " +
                        "WHERE status = 'pending' ORDER BY created_at LIMIT ?",
                maxItems);

        if (!items.isEmpty()) {
            List<Object> ids = items.stream().map(m -> m.get("id")).toList();
            String placeholders = ids.stream().map(i -> "?").reduce((a, b) -> a + "," + b).orElse("");
            jdbc.update(
                    "UPDATE ingest_queue SET status = 'processing' WHERE id IN (" + placeholders + ")",
                    ids.toArray());
        }

        return items;
    }

    public void markDone(int id) {
        jdbc.update("UPDATE ingest_queue SET status = 'done', processed_at = NOW() WHERE id = ?", id);
    }

    public void markError(int id, String message) {
        String truncated = message != null && message.length() > 500 ? message.substring(0, 500) : message;
        jdbc.update("UPDATE ingest_queue SET status = 'error', error_message = ?, processed_at = NOW() WHERE id = ?",
                truncated, id);
    }

    public int countPending() {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM ingest_queue WHERE status = 'pending'", Integer.class);
        return count != null ? count : 0;
    }
}
