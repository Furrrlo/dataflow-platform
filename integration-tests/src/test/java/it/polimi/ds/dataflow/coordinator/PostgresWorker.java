package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.worker.dfs.WorkerDfs;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

public record PostgresWorker(String postgresNodeName,
                             PostgreSQLContainer<?> container,
                             DataSource ds,
                             WorkerDfs dfs) {

    public PostgreSQLContainer<?> getContainer() {
        return container;
    }

    public WorkerDfs getDfs() {
        return dfs;
    }
}
