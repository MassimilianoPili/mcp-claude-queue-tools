package io.github.massimilianopili.mcp.queue;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class QueueConfig {

    @Bean(name = "taskQueueDataSource")
    public HikariDataSource taskQueueDataSource(QueueProperties props) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(props.getDbUrl());
        ds.setUsername(props.getDbUsername());
        if (props.getDbCredential() != null) {
            ds.setPassword(props.getDbCredential());
        }
        ds.setMaximumPoolSize(props.getDbPoolSize());
        ds.setMinimumIdle(props.getDbMinimumIdle());
        ds.setConnectionTimeout(props.getDbConnectionTimeout());
        ds.setLeakDetectionThreshold(props.getDbLeakDetectionThreshold());
        ds.setPoolName("taskqueue-pool");
        return ds;
    }
}
