package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.dfs.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

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
    static Worker WORKER1;

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER2_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-worker2")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");
    @SuppressWarnings("NullAway.Init") // Initialized in a weird way
    static Worker WORKER2;

    static List<Worker> WORKERS;

    record Worker(String postgresNodeName, PostgreSQLContainer<?> container, DataSource ds, Dfs dfs) {
    }

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

        COORDINATOR_DFS = new PostgresCoordinatorDfs(serde, config -> {
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setUrl(COORDINATOR_NODE.getJdbcUrl());
            ds.setUser(COORDINATOR_NODE.getUsername());
            ds.setPassword(COORDINATOR_NODE.getPassword());
            ds.setDatabaseName(COORDINATOR_NODE.getDatabaseName());
            config.setDataSource(ds);
        });

        record TempWorker(String name, PostgreSQLContainer<?> container, Consumer<Worker> workerSetter) {
        }

        WORKERS = Stream.of(
                new TempWorker("worker1", WORKER1_NODE, (w) -> WORKER1 = w),
                new TempWorker("worker2", WORKER2_NODE, (w) -> WORKER2 = w)
        ).map(w -> {
            var ds = new PGSimpleDataSource();
            ds.setUrl(w.container.getJdbcUrl());
            ds.setUser(w.container.getUsername());
            ds.setPassword(w.container.getPassword());
            ds.setDatabaseName(w.container.getDatabaseName());
            return new Worker(w.name, w.container, ds, new PostgresDfs("coordinator", serde, config -> {
                config.setDataSource(ds);
            }));
        }).toList();

        WORKERS.forEach(w -> COORDINATOR_DFS.addForeignServer(w.postgresNodeName()));

        PGSimpleDataSource coordinatorDs = new PGSimpleDataSource();
        coordinatorDs.setUrl(COORDINATOR_NODE.getJdbcUrl());
        coordinatorDs.setUser(COORDINATOR_NODE.getUsername());
        coordinatorDs.setPassword(COORDINATOR_NODE.getPassword());
        coordinatorDs.setDatabaseName(COORDINATOR_NODE.getDatabaseName());

        try(Connection connection = coordinatorDs.getConnection();
            Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw");

            for (var workerEntry : WORKERS) {
                var workerNodeName = workerEntry.postgresNodeName();
                var worker = workerEntry.container;

                statement.execute(STR."""
                    CREATE SERVER IF NOT EXISTS \{workerNodeName} FOREIGN DATA WRAPPER postgres_fdw
                    OPTIONS (host '\{worker.getNetworkAliases().getFirst()}', dbname '\{worker.getDatabaseName()}');

                    CREATE USER MAPPING IF NOT EXISTS FOR \{COORDINATOR_NODE.getUsername()} SERVER \{workerNodeName}
                    OPTIONS (user '\{worker.getUsername()}', password '\{worker.getPassword()}');
                    """);
            }
        }

        for (var workerEntry : WORKERS) {
            var workerNode = workerEntry.container;

            PGSimpleDataSource workerDs = new PGSimpleDataSource();
            workerDs.setUrl(workerNode.getJdbcUrl());
            workerDs.setUser(workerNode.getUsername());
            workerDs.setPassword(workerNode.getPassword());
            workerDs.setDatabaseName(workerNode.getDatabaseName());

            try(Connection connection = workerDs.getConnection();
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
    @SuppressWarnings("unused")
    static void tearDown() throws IOException {
        var exs = new ArrayList<Throwable>();
        WORKERS.forEach(w -> {
            try {
                w.container.close();
            } catch (Throwable t) {
                exs.add(t);
            }
        });

        try(var coordinator = COORDINATOR_NODE; var network = NETWORK) {
            if(exs.isEmpty())
                return;

            IOException ex = new IOException();
            exs.forEach(ex::addSuppressed);
            throw ex;
        }
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
            Worker worker = WORKERS.stream()
                    .filter(w -> partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(STR."Missing node for partition \{partition}"));
            worker.dfs.createFilePartition(partition.fileName(), partition.partition());

            var workerFile = worker.dfs.findFile(partition.fileName());
            tuplesToWrite.forEach(t -> {
                // the serde is sidestepped here, it just writes whatever json is in the key
                var tuple = new Tuple2("{" +
                        STR."\"key\": \"\{t.key()}\", " +
                        STR."\"value\": \"\{t.value()}\", " +
                        STR."\"partition\": \"\{partition.partition()}\"" +
                        "}", "");
                worker.dfs.writeInPartition(workerFile, tuple, partition.partition());
                tuplesToRead.add(tuple);
            });
        }

        var tupleComparator = Comparator.<Tuple2, String>comparing(t -> (String) t.key());
        assertEquals(
                tuplesToRead.stream().sorted(tupleComparator).toList(),
                COORDINATOR_DFS.loadAll(coordinatorPartitionedFile).sorted(tupleComparator).toList());

        // Try to write from one which does not own the partition
        for (DfsFilePartitionInfo partition : coordinatorPartitionedFile.partitions()) {
            Worker worker = WORKERS.stream()
                    .filter(w -> !partition.dfsNodeName().equals(w.postgresNodeName()))
                    .findFirst()
                    .orElse(null);
            if(worker == null)
                continue;

            var workerFile = worker.dfs.findFile(partition.fileName());
            tuplesToWrite.forEach(t -> {
                // the serde is sidestepped here, it just writes whatever json is in the key
                var tuple = new Tuple2("{" +
                        STR."\"key\": \"\{t.key()}\", " +
                        STR."\"value\": \"\{t.value()}\", " +
                        STR."\"partition\": \"\{partition.partition()}\"" +
                        "}", "");
                worker.dfs.writeInPartition(workerFile, tuple, partition.partition());
                tuplesToRead.add(tuple);
            });
        }

        assertEquals(
                tuplesToRead.stream().sorted(tupleComparator).toList(),
                COORDINATOR_DFS.loadAll(coordinatorPartitionedFile).sorted(tupleComparator).toList());
    }
}