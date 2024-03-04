package it.polimi.ds.dataflow.dfs;

import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

public final class TestcontainerUtil {

    private TestcontainerUtil() {
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
