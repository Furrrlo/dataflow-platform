package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.worker.dfs.WorkerDfs;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

public record PostgresWorker(String postgresNodeName,
                             PostgreSQLContainer<?> container,
                             DataSource ds,
                             WorkerDfs dfs) implements Closeable {

    public PostgreSQLContainer<?> getContainer() {
        return container;
    }

    public WorkerDfs getDfs() {
        return dfs;
    }

    @Override
    @SuppressWarnings("EmptyTryBlock")
    public void close() throws IOException {
        try(var _ = this.container;
            var _ = ds instanceof Closeable dsc ? dsc : null;
            var _ = this.dfs) {
            // Just want to close all of them
        }
    }
}
