package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

public record PostgresWorker(String postgresNodeName, PostgreSQLContainer<?> container, DataSource ds,
                             PostgresWorkerDfs dfs) {

    public PostgreSQLContainer<?> getContainer() {
        return container;
    }

    public Dfs getDfs() {
        return dfs;
    }

    public static DataSource createDataSourceFor(PostgreSQLContainer<?> container) {
        PGSimpleDataSource workerDs = new PGSimpleDataSource();
        workerDs.setUrl(container.getJdbcUrl());
        workerDs.setUser(container.getUsername());
        workerDs.setPassword(container.getPassword());
        workerDs.setDatabaseName(container.getDatabaseName());
        return workerDs;
    }
}
