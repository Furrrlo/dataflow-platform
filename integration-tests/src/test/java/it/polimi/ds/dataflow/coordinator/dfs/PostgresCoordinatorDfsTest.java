package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.PostgresWorker;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.Closeables;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.coordinator.PostgresWorker.createDataSourceFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.abort;

@Testcontainers(disabledWithoutDocker = true)
class PostgresCoordinatorDfsTest {

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
    static PostgresWorker WORKER1;

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER2_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-worker2")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");
    @SuppressWarnings("NullAway.Init") // Initialized in a weird way
    static PostgresWorker WORKER2;

    static List<PostgresWorker> WORKERS;

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

        record TempWorker(String name, PostgreSQLContainer<?> container, Consumer<PostgresWorker> workerSetter) {
        }

        WORKERS = Stream.of(
                new TempWorker("worker1", WORKER1_NODE, (w) -> WORKER1 = w),
                new TempWorker("worker2", WORKER2_NODE, (w) -> WORKER2 = w)
        ).map(w -> {
            var ds = createDataSourceFor(w.container);
            return new PostgresWorker(w.name, w.container, ds, new PostgresDfs(
                    "coordinator", serde,
                    config -> config.setDataSource(ds)));
        }).toList();

        WORKERS.forEach(w -> COORDINATOR_DFS.addForeignServer(w.postgresNodeName()));

        try(Connection connection = createDataSourceFor(COORDINATOR_NODE).getConnection();
            Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw");

            for (var workerEntry : WORKERS) {
                var workerNodeName = workerEntry.postgresNodeName();
                var worker = workerEntry.getContainer();

                statement.execute(STR."""
                    CREATE SERVER IF NOT EXISTS \{workerNodeName} FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host '\{worker.getNetworkAliases().getFirst()}', dbname '\{worker.getDatabaseName()}');

                    CREATE USER MAPPING IF NOT EXISTS FOR \{COORDINATOR_NODE.getUsername()} SERVER \{workerNodeName}
                    OPTIONS (user '\{worker.getUsername()}', password '\{worker.getPassword()}');
                    """);
            }
        }

        for (var workerEntry : WORKERS) {
            var workerNode = workerEntry.getContainer();

            try(Connection connection = createDataSourceFor(workerNode).getConnection();
                Statement statement = connection.createStatement()) {
                statement.execute(STR."""
                    CREATE EXTENSION IF NOT EXISTS postgres_fdw;
                    
                    CREATE SERVER IF NOT EXISTS coordinator FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host '\{COORDINATOR_NODE.getNetworkAliases().getFirst()}', dbname '\{COORDINATOR_NODE.getDatabaseName()}');
                    
                    CREATE USER MAPPING IF NOT EXISTS FOR \{workerNode.getUsername()} SERVER coordinator
                    OPTIONS (user '\{COORDINATOR_NODE.getUsername()}', password '\{COORDINATOR_NODE.getPassword()}');
                    """);
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        Closeables.Auto.closeAll(Stream.concat(
                WORKERS.stream().map(PostgresWorker::container),
                Stream.of(COORDINATOR_NODE, NETWORK)),
                e -> new Exception("Failed to tear down stuff", e));
    }

    @Test
    void createAndFindPartitionedFile() {
        String file = "createAndFindPartitionedFile";
        DfsFile partitionedFile = COORDINATOR_DFS.createPartitionedFile(file, 4);
        assertEquals(partitionedFile, COORDINATOR_DFS.findFile(file));
    }

    @Test
    void createAndWritePartitionedFileAndPartitions() {
        String file = "createAndWritePartitionedFileAndPartitions";
        DfsFile coordinatorPartitionedFile;
        try {
            coordinatorPartitionedFile = COORDINATOR_DFS.createPartitionedFile(file, 4);
        } catch (Throwable t) {
            abort("Partitioned file test should have failed");
            return;
        }

        var tuplesToWrite = List.of(
                new Tuple2("key1", "value1"),
                new Tuple2("key2", "value2"),
                new Tuple2("key3", "value2"));
        var tuplesToRead = new ArrayList<Tuple2>();

        // Create the file partition on the correct workers and try to write into it
        for (DfsFilePartitionInfo partition : coordinatorPartitionedFile.partitions()) {
            PostgresWorker worker = WORKERS.stream()
                    .filter(w -> partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(STR."Missing node for partition \{partition}"));
            worker.getDfs().createFilePartition(partition.fileName(), partition.partition());

            var workerFile = worker.getDfs().findFile(partition.fileName());
            tuplesToWrite.forEach(t -> {
                // the serde is sidestepped here, it just writes whatever json is in the key
                var tuple = new Tuple2("{" +
                        STR."\"key\": \"\{t.key()}\", " +
                        STR."\"value\": \"\{t.value()}\", " +
                        STR."\"partition\": \"\{partition.partition()}\"" +
                        "}", "");
                worker.getDfs().writeInPartition(workerFile, tuple, partition.partition());
                tuplesToRead.add(tuple);
            });
        }

        var tupleComparator = Comparator.<Tuple2, String>comparing(t -> (String) t.key());
        assertEquals(
                tuplesToRead.stream().sorted(tupleComparator).toList(),
                COORDINATOR_DFS.loadAll(coordinatorPartitionedFile).sorted(tupleComparator).toList());

        // Try to write from one which does not own the partition
        for (DfsFilePartitionInfo partition : coordinatorPartitionedFile.partitions()) {
            PostgresWorker worker = WORKERS.stream()
                    .filter(w -> !partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElse(null);
            if(worker == null)
                continue;

            var workerFile = worker.getDfs().findFile(partition.fileName());
            tuplesToWrite.forEach(t -> {
                // the serde is sidestepped here, it just writes whatever json is in the key
                var tuple = new Tuple2("{" +
                        STR."\"key\": \"\{t.key()}\", " +
                        STR."\"value\": \"\{t.value()}\", " +
                        STR."\"partition\": \"\{partition.partition()}\"" +
                        "}", "");
                worker.getDfs().writeInPartition(workerFile, tuple, partition.partition());
                tuplesToRead.add(tuple);
            });
        }

        assertEquals(
                tuplesToRead.stream().sorted(tupleComparator).toList(),
                COORDINATOR_DFS.loadAll(coordinatorPartitionedFile).sorted(tupleComparator).toList());
    }
}