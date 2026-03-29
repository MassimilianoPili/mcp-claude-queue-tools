package io.github.massimilianopili.mcp.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mcp.taskqueue")
public class QueueProperties {

    // DB
    private String dbUrl = "jdbc:postgresql://postgres:5432/embeddings";
    private String dbUsername = "postgres";
    private String dbCredential;
    private int dbPoolSize = 30;
    private int dbMinimumIdle = 0;
    private long dbConnectionTimeout = 10_000;
    private long dbLeakDetectionThreshold = 60_000;

    // Preference Sort
    private String preferenceSortApiUrl = "http://preference-sort:8093";
    private String preferenceSortUserId = "f7294891-b031-432d-8382-8592d3e6b1aa";

    // Redis
    private String redisKey = "claude:taskq";

    // Defaults
    private int defaultPriority = 5;
    private int progressPriority = 10;
    private int defaultLimit = 20;
    private int maxLimit = 500;

    // Category
    private String taskQueueCategory = "task-queue";
    private double igThreshold = 0.01;

    // DB getters/setters
    public String getDbUrl() { return dbUrl; }
    public void setDbUrl(String dbUrl) { this.dbUrl = dbUrl; }

    public String getDbUsername() { return dbUsername; }
    public void setDbUsername(String dbUsername) { this.dbUsername = dbUsername; }

    public String getDbCredential() { return dbCredential; }
    public void setDbCredential(String dbCredential) { this.dbCredential = dbCredential; }

    public int getDbPoolSize() { return dbPoolSize; }
    public void setDbPoolSize(int dbPoolSize) { this.dbPoolSize = dbPoolSize; }

    public int getDbMinimumIdle() { return dbMinimumIdle; }
    public void setDbMinimumIdle(int dbMinimumIdle) { this.dbMinimumIdle = dbMinimumIdle; }

    public long getDbConnectionTimeout() { return dbConnectionTimeout; }
    public void setDbConnectionTimeout(long dbConnectionTimeout) { this.dbConnectionTimeout = dbConnectionTimeout; }

    public long getDbLeakDetectionThreshold() { return dbLeakDetectionThreshold; }
    public void setDbLeakDetectionThreshold(long dbLeakDetectionThreshold) { this.dbLeakDetectionThreshold = dbLeakDetectionThreshold; }

    // Preference Sort getters/setters
    public String getPreferenceSortApiUrl() { return preferenceSortApiUrl; }
    public void setPreferenceSortApiUrl(String preferenceSortApiUrl) { this.preferenceSortApiUrl = preferenceSortApiUrl; }

    public String getPreferenceSortUserId() { return preferenceSortUserId; }
    public void setPreferenceSortUserId(String preferenceSortUserId) { this.preferenceSortUserId = preferenceSortUserId; }

    // Redis getters/setters
    public String getRedisKey() { return redisKey; }
    public void setRedisKey(String redisKey) { this.redisKey = redisKey; }

    // Defaults getters/setters
    public int getDefaultPriority() { return defaultPriority; }
    public void setDefaultPriority(int defaultPriority) { this.defaultPriority = defaultPriority; }

    public int getProgressPriority() { return progressPriority; }
    public void setProgressPriority(int progressPriority) { this.progressPriority = progressPriority; }

    public int getDefaultLimit() { return defaultLimit; }
    public void setDefaultLimit(int defaultLimit) { this.defaultLimit = defaultLimit; }

    public int getMaxLimit() { return maxLimit; }
    public void setMaxLimit(int maxLimit) { this.maxLimit = maxLimit; }

    // Category getters/setters
    public String getTaskQueueCategory() { return taskQueueCategory; }
    public void setTaskQueueCategory(String taskQueueCategory) { this.taskQueueCategory = taskQueueCategory; }

    public double getIgThreshold() { return igThreshold; }
    public void setIgThreshold(double igThreshold) { this.igThreshold = igThreshold; }
}
