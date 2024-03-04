package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.Closeables;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.TestcontainerUtil.createDataSourceFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class PostgresWorkerDfsTest {

    static final Tuple2JsonSerde SERDE = new Tuple2JsonSerde() {
        @Override
        public String jsonify(Tuple2 t) {
            return t.key().toString();
        }

        @Override
        public Tuple2 parseJson(String json) {
            return new Tuple2(json, "");
        }
    };

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");
    @SuppressWarnings("NullAway.Init") // Initialized in a weird way
    static WorkerDfs DFS;

    @BeforeAll
    static void beforeAll() {
        var ds = createDataSourceFor(WORKER_NODE);
        DFS = new PostgresWorkerDfs(
                "coordinator",
                UUID.randomUUID(),
                SERDE,
                config -> config.setDataSource(ds));
    }

    @AfterAll
    static void tearDown() throws Exception {
        Closeables.Auto.closeAll(
                Stream.of(DFS, WORKER_NODE),
                e -> new Exception("Failed to tear down stuff", e));
    }

    @BeforeEach
    void beforeEach() throws SQLException {
        try (var conn = createDataSourceFor(WORKER_NODE).getConnection()) {
            TestBackupInfo.truncate(DSL.using(conn));
        }
    }

    @Test
    void createAndInsertAndUpdateBackupFile() throws IOException, SQLException {
        // Initializing a bunch of UUIDs
        var uuids = List.of(UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID());
        var dfses = uuids.stream().map(uuid -> {
            var ds = createDataSourceFor(WORKER_NODE);
            return new PostgresWorkerDfs(
                    "coordinator",
                    uuid,
                    SERDE,
                    config -> config.setDataSource(ds));
        }).toList();

        var infoToRead = new ArrayList<TestBackupInfo>();
        try(var _ = Closeables.compose(dfses)) {
            var infoToWrite = List.of(
                    new TestBackupInfo(uuids.get(0), 1, 1, 1),
                    new TestBackupInfo(uuids.get(1), 2, 2, 2),
                    new TestBackupInfo(uuids.get(2), 3, 3, 3),
                    new TestBackupInfo(uuids.get(3), 4, 4, 4));

            // Write a bunch of rows to be modified in the backup table
            for (int i = 0; i < infoToWrite.size(); i++) {
                var info = infoToWrite.get(i);
                var dfs = dfses.get(i);

                dfs.writeBackupInfo(info.jobId(), info.partition(), null);
            }

            // Update the rows previously written
            for (int i = 0; i < infoToWrite.size(); i++) {
                var info = infoToWrite.get(i);
                var dfs = dfses.get(i);

                dfs.writeBackupInfo(info.jobId(), info.partition(), info.nextBatchPtr());
                infoToRead.add(info);
            }
        }

        // Declaring comparator to reorder BackupInfos according to their UUID's conversion to string
        var infoComparator = Comparator.<TestBackupInfo, String>comparing(i -> i.uuid().toString());
        try (var conn = createDataSourceFor(WORKER_NODE).getConnection()) {
            DSLContext ctx = DSL.using(conn);
            assertEquals(
                    infoToRead.stream().sorted(infoComparator).toList(),
                    TestBackupInfo.loadAllFrom(ctx).sorted(infoComparator).toList());
        }
    }

    @Test
    void createAndDeleteRowBackupFile() throws SQLException {
        // Inserting a row
        DFS.writeBackupInfo(1, 1, 1);
        // Deleting the row
        DFS.deleteBackup(1, 1);
        // Table should be empty
        try (var conn = createDataSourceFor(WORKER_NODE).getConnection()) {
            DSLContext ctx = DSL.using(conn);
            assertEquals(
                    Collections.EMPTY_LIST,
                    TestBackupInfo.loadAllFrom(ctx).toList());
        }
    }
}
