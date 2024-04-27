package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.Closeables;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.TestcontainerUtil.createDataSourceFor;
import static it.polimi.ds.dataflow.socket.packets.HelloPacket.PreviousJob;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class PostgresWorkerDfsTest {

    static final Tuple2JsonSerde SERDE = new Tuple2JsonSerde() {
        @Override
        public String jsonify(Tuple2 t) {
            return t.key().toString();
        }

        @Override
        public String jsonifyJsObj(@Nullable Object t) {
            return Objects.toString(t);
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
        try (var _ = Closeables.compose(dfses)) {
            var infoToWrite = List.of(
                    new TestBackupInfo(uuids.get(0),1, 1, "yay_1",  1),
                    new TestBackupInfo(uuids.get(1),2, 2, "yay_2",  2),
                    new TestBackupInfo(uuids.get(2),3, 3, "yay_3",  3),
                    new TestBackupInfo(uuids.get(3),4, 4, "yay_4",  4));

            // Write a bunch of rows to be modified in the backup table
            for (int i = 0; i < infoToWrite.size(); i++) {
                var info = infoToWrite.get(i);
                var dfs = dfses.get(i);

                var dfsInfo = new DfsFilePartitionInfo(
                        "yay", info.partitionFileName(), info.partition(), "localhost", true);
                dfs.writeBackupInfo(info.jobId(), info.partition(),  dfsInfo, null);
            }

            // Update the rows previously written
            for (int i = 0; i < infoToWrite.size(); i++) {
                var info = infoToWrite.get(i);
                var dfs = dfses.get(i);

                var dfsInfo = new DfsFilePartitionInfo(
                        "yay", info.partitionFileName(), info.partition(), "localhost", true);
                dfs.writeBackupInfo(info.jobId(), info.partition(), dfsInfo, info.nextBatchPtr());
                infoToRead.add(info);
            }
        }

        // Declaring comparator to reorder BackupInfos in order to compare them.
        // (Added more comparator just for extreme safety, they shouldn't be needed)
        var infoComparator = Comparator.comparing(TestBackupInfo::uuid)
                .thenComparing(TestBackupInfo::jobId)
                .thenComparing(TestBackupInfo::partition)
                .thenComparing(TestBackupInfo::nextBatchPtr);
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
        DFS.writeBackupInfo(
                1, 1,
                new DfsFilePartitionInfo(
                        "yay", "yay_1", 1, "localhost", true),
                1);
        // Deleting the row
        DFS.deleteBackupAndFiles(1, List.of());
        // Table should be empty
        try (var conn = createDataSourceFor(WORKER_NODE).getConnection()) {
            DSLContext ctx = DSL.using(conn);
            assertEquals(
                    Collections.EMPTY_LIST,
                    TestBackupInfo.loadAllFrom(ctx).toList());
        }
    }

    @Test
    void readWorkerPreviousJobs() {
        UUID uuid = UUID.randomUUID();
        var infoToWrite = List.of(
                new TestBackupInfo(uuid,1, 1, "yay_1", 1),
                new TestBackupInfo(uuid,1, 2, "yay_2", null),
                new TestBackupInfo(uuid,2, 3, "yay_3", 10),
                new TestBackupInfo(uuid,2, 7, "yay_7", 20),
                new TestBackupInfo(uuid,3, 7, "yay_7", 30),
                new TestBackupInfo(uuid,3, 1, "yay_1", 40)
        );
        var infoToRead = infoToWrite.stream().map(info -> new PreviousJob(info.jobId(), info.partition())).toList();
        TestBackupInfo.writeMultipleBackupInfos(DFS, infoToWrite);

        var previousJobsComparator = Comparator.comparingInt(PreviousJob::jobId)
                .thenComparing(PreviousJob::partition);

        var previousJobs = DFS.readWorkerJobs();

        assertEquals(
                infoToRead.stream().sorted(previousJobsComparator).toList(),
                previousJobs.stream().sorted(previousJobsComparator).toList());

    }
}
