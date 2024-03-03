package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.PostgresWorker;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.Closeables;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.coordinator.PostgresWorker.createDataSourceFor;
import static it.polimi.ds.dataflow.worker.dfs.WorkerDfs.BackupInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class PostgresWorkerDfsTest {
    static final Network NETWORK = Network.newNetwork();
    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> COORDINATOR_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-coordinator")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");
    static PostgresCoordinatorDfs COORDINATOR_DFS;
    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER1_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-worker1")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");
    @SuppressWarnings("NullAway.Init") // Initialized in a weird way
    static PostgresWorker WORKER;

    @BeforeAll
    @SuppressWarnings({"SqlResolve", "TrailingWhitespacesInTextBlock"})
    static void beforeAll() throws SQLException {
        var serde = new Tuple2JsonSerde() {
            @Override
            public String jsonify(Tuple2 t) {
                return t.key().toString();
            }

            @Override
            public Tuple2 parseJson(String json) {
                return new Tuple2(json, "");
            }
        };
        COORDINATOR_DFS = new PostgresCoordinatorDfs(
                serde,
                config -> config.setDataSource(createDataSourceFor(COORDINATOR_NODE)));

        var ds = createDataSourceFor(WORKER1_NODE);
        WORKER = new PostgresWorker("worker", WORKER1_NODE, ds, new PostgresWorkerDfs(
                "coordinator", serde,
                config -> config.setDataSource(ds)
        ));
        COORDINATOR_DFS.addForeignServer(WORKER.postgresNodeName());

        try (Connection connection = PostgresWorker.createDataSourceFor(COORDINATOR_NODE).getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw");


            statement.execute(STR."""
                    CREATE SERVER IF NOT EXISTS \{WORKER.postgresNodeName()} FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host '\{WORKER1_NODE.getNetworkAliases().getFirst()}', dbname '\{WORKER1_NODE.getDatabaseName()}');

                    CREATE USER MAPPING IF NOT EXISTS FOR \{COORDINATOR_NODE.getUsername()} SERVER \{WORKER.postgresNodeName()}
                    OPTIONS (user '\{WORKER1_NODE.getUsername()}', password '\{WORKER1_NODE.getPassword()}');
                    """);

        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        Closeables.Auto.closeAll(
                Stream.of(WORKER1_NODE, NETWORK),
                e -> new Exception("Failed to tear down stuff", e));
    }

    @Test
    void createAndFindBackupFile() {
        String name = "createAndFindBackupFile";
        WORKER.dfs().createBackupFile(name);
        assertEquals(name, WORKER.dfs().findFile(name));
    }

    @Test
    void createAndInsertAndUpdateBackupFile() {
        String name = "createAndUpdateBackupFile";
        WORKER.dfs().createBackupFile(name);

        //Initializing a bunch of UUIDs
        var uuids = List.of(UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID());

        //Write a bunch of rows to be modified in the backup table
        uuids.forEach(e ->
                WORKER.dfs().writeBackupInfo(name,
                        new BackupInfo(e,
                                0,
                                0,
                                0)));


        var infoToWrite = List.of(
                new BackupInfo(uuids.get(0), 1, 1, 1),
                new BackupInfo(uuids.get(1), 2, 2, 2),
                new BackupInfo(uuids.get(2), 3, 3, 3),
                new BackupInfo(uuids.get(3), 4, 4, 4)
        );
        var infoToRead = new ArrayList<BackupInfo>();

        //Update the rows previously written
        for (BackupInfo info : infoToWrite) {
            WORKER.dfs().updateBackupFile(name, info);
            infoToRead.add(info);
        }

        //Declaring comparator to reorder BackupInfos according to their UUID's
        //conversion to string
        var infoComparator = Comparator.<BackupInfo, String>comparing(i -> i.uuid().toString());
        try (var res = WORKER.dfs().loadAll(name)) {
            assertEquals(
                    infoToRead.stream().sorted(infoComparator).toList(),
                    res.sorted(infoComparator).toList()
            );
        }
    }

    @Test
    void createAndDeleteRowBackupFile() {
        String name = "createAndDeleteBackupFile";
        //Creating empty backup table
        WORKER.dfs().createBackupFile(name);

        UUID toDelete = UUID.randomUUID();
        //Inserting a row
        WORKER.dfs().writeBackupInfo(name,new BackupInfo(toDelete,
                1,1,1));

        //Deleting the row
        WORKER.dfs().deleteBackupFile(name,toDelete);
        //Table should be empty
        try(var res = WORKER.dfs().loadAll(name)) {
            assertEquals(Collections.EMPTY_LIST, res.toList());
        }
    }
}
