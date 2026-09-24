package com.h2;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Starts a throwaway PostgreSQL in Docker for the tests and points the datasource at it,
 * so tests need neither a running database nor imported data.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return newPostgresContainer();
    }

    // Same image and schema script as docker-compose.yml, plus a few known books for the tests to find.
    // Static so tests that don't start Spring (e.g. DBImporterTest) can use the same database setup.
    static PostgreSQLContainer<?> newPostgresContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
                .withCopyFileToContainer(MountableFile.forHostPath("db/create_schema.sql"),
                        "/docker-entrypoint-initdb.d/01-schema.sql")
                .withCopyFileToContainer(MountableFile.forClasspathResource("test-data.sql"),
                        "/docker-entrypoint-initdb.d/02-test-data.sql");
    }
}
