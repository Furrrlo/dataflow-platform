package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.dfs.Dfs;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

public record PostgresWorker(String postgresNodeName, PostgreSQLContainer<?> container, DataSource ds, Dfs dfs) {

    public PostgreSQLContainer<?> getContainer() {
        return container;
    }
}
