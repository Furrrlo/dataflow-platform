package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.PostgresWorker;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.Closeables;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import org.jetbrains.annotations.Nullable;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.TestcontainerUtil.createDataSourceFor;
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
            return new PostgresWorker(w.name, w.container, ds, new PostgresWorkerDfs(
                    "coordinator", UUID.randomUUID(), serde,
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
    void createPartitionedFilePreemptively() {
        String file = "createPartitionedFilePreemptively";
        DfsFile partitionedFile = COORDINATOR_DFS.createPartitionedFilePreemptively(file, 4);
        assertEquals(partitionedFile, COORDINATOR_DFS.findFile(file));
    }

    @Test
    void createPreemptivelyAndWritePartitionedFileAndPartitions() {
        String file = "createPreemptivelyAndWritePartitionedFileAndPartitions";
        DfsFile coordinatorPartitionedFile;
        try {
            coordinatorPartitionedFile = COORDINATOR_DFS.createPartitionedFilePreemptively(file, 4);
        } catch (Throwable t) {
            abort("Partitioned file test should have failed");
            return;
        }

        // Create the file partition on the correct workers and try to write into it
        for (DfsFilePartitionInfo partition : coordinatorPartitionedFile.partitions()) {
            PostgresWorker worker = WORKERS.stream()
                    .filter(w -> partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(STR."Missing node for partition \{partition}"));
            worker.getDfs().createFilePartition(partition.fileName(), partition.partition());
        }

        List<Tuple2> tuplesToRead = new ArrayList<>();
        tuplesToRead = doWriteOwnedPartition(coordinatorPartitionedFile, coordinatorPartitionedFile.partitions(), tuplesToRead);
        doWriteNonOwnedPartition(coordinatorPartitionedFile, tuplesToRead);
    }

    @Test
    void createAndWritePartitionedFileAndPartitions() {
        String file = "cawpfap"; // Otherwise we exceed the name length
        int partitionsNum = 4;

        // Create the file partition on the correct workers and try to write into it
        var partitions = new ArrayList<DfsFilePartitionInfo>();
        for (int partitionIdx = 0; partitionIdx < partitionsNum; partitionIdx++) {
            PostgresWorker worker = WORKERS.get(partitionIdx % WORKERS.size());
            String partitionFileName =
                    file + "_" + worker.postgresNodeName() + "_" + System.currentTimeMillis() + "_" + partitionIdx;
            worker.getDfs().createFilePartition(file, partitionFileName, partitionIdx);
            partitions.add(new DfsFilePartitionInfo(
                    file, partitionFileName, partitionIdx, worker.postgresNodeName(), false));
        }

        List<Tuple2> tuplesToRead = new ArrayList<>();
        tuplesToRead = doWriteOwnedPartition(null, partitions, tuplesToRead);

        DfsFile coordinatorPartitionedFile = COORDINATOR_DFS.createPartitionedFile(file, partitions);
        var tupleComparator = Comparator.<Tuple2, String>comparing(t -> (String) t.key());
        try (var res = COORDINATOR_DFS.loadAll(coordinatorPartitionedFile)) {
            assertEquals(
                    tuplesToRead.stream().sorted(tupleComparator).toList(),
                    res.sorted(tupleComparator).toList());
        }

        doWriteNonOwnedPartition(coordinatorPartitionedFile, tuplesToRead);
    }

    private List<Tuple2> doWriteOwnedPartition(@Nullable DfsFile coordinatorPartitionedFile,
                                               SequencedCollection<DfsFilePartitionInfo> coordinatorPartitions,
                                               List<Tuple2> tuplesToAlreadyRead) {
        var tuplesToWrite = List.of(
                new Tuple2("key1", "value1"),
                new Tuple2("key2", "value2"),
                new Tuple2("key3", "value2"));
        var tuplesToRead = new ArrayList<>(tuplesToAlreadyRead);

        // Try to write into it
        for (DfsFilePartitionInfo partition : coordinatorPartitions) {
            PostgresWorker worker = WORKERS.stream()
                    .filter(w -> partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(STR."Missing node for partition \{partition}"));

            var workerFile = worker.getDfs().findFile(partition.fileName(), coordinatorPartitions.size());
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

        if(coordinatorPartitionedFile != null) {
            var tupleComparator = Comparator.<Tuple2, String>comparing(t -> (String) t.key());
            try (var res = COORDINATOR_DFS.loadAll(coordinatorPartitionedFile)) {
                assertEquals(
                        tuplesToRead.stream().sorted(tupleComparator).toList(),
                        res.sorted(tupleComparator).toList());
            }
        }

        return tuplesToRead;
    }

    private void doWriteNonOwnedPartition(DfsFile coordinatorPartitionedFile, List<Tuple2> tuplesToAlreadyRead) {
        var tuplesToWrite = List.of(
                new Tuple2("key1", "value1"),
                new Tuple2("key2", "value2"),
                new Tuple2("key3", "value2"));
        var tuplesToRead = new ArrayList<>(tuplesToAlreadyRead);

        // Try to write from one which does not own the partition
        for (DfsFilePartitionInfo partition : coordinatorPartitionedFile.partitions()) {
            PostgresWorker worker = WORKERS.stream()
                    .filter(w -> !partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElse(null);
            if(worker == null)
                continue;

            var workerFile = worker.getDfs().findFile(partition.fileName(), coordinatorPartitionedFile.partitionsNum());
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
        try(var res = COORDINATOR_DFS.loadAll(coordinatorPartitionedFile)) {
            assertEquals(
                    tuplesToRead.stream().sorted(tupleComparator).toList(),
                    res.sorted(tupleComparator).toList());
        }
    }
}