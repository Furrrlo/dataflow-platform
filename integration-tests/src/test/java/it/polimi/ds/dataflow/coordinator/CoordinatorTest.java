package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.dfs.PostgresCoordinatorDfs;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.Closeables;
import it.polimi.ds.dataflow.utils.SimpleScriptEngineFactory;
import it.polimi.ds.dataflow.worker.SimulateCrashException;
import it.polimi.ds.dataflow.worker.Worker;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.junit.jupiter.api.*;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.TestcontainerUtil.createDataSourceFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class CoordinatorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorTest.class);

    static final WorkDirFileLoader FILE_LOADER = new WorkDirFileLoader(Paths.get("./"));

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
    static Coordinator COORDINATOR;

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER1_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-worker1")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");

    @Container
    @SuppressWarnings("resource")
    static final PostgreSQLContainer<?> WORKER2_NODE = new PostgreSQLContainer<>("postgres:15.6")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres-worker2")
            .withUsername("postgres")
            .withPassword("password")
            .withDatabaseName("postgres");

    static List<PostgresWorker> POSTGRES_WORKERS;
    static List<Worker> WORKERS;

    static final ExecutorService IO_THREAD_POOL = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
            .name("worker-io-", 0)
            .factory());
    static final ExecutorService CPU_THREAD_POOL = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            Thread.ofPlatform()
                    .name("worker-cpu-", 0)
                    .factory());

    @BeforeAll
    @SuppressWarnings({"SqlResolve", "TrailingWhitespacesInTextBlock"})
    static void beforeAll() throws Exception {
        final SimpleScriptEngineFactory engineFactory = SimpleScriptEngineFactory.wrap(
                new NashornScriptEngineFactory(),
                f -> f.getScriptEngine("--language=es6", "-doe"));

        int port;
        try(ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        WorkerManager workerManager;
        COORDINATOR = new Coordinator(
                FILE_LOADER,
                Parser.create("--language=es6"),
                workerManager = WorkerManager.listen(IO_THREAD_POOL, port, null),
                COORDINATOR_DFS = new PostgresCoordinatorDfs(
                        engineFactory.create(),
                        config -> config.setDataSource(createDataSourceFor(COORDINATOR_NODE)),
                        workerManager::registerForeignServersUpdater));

        POSTGRES_WORKERS = new ArrayList<>();
        for (var e : Map.of(
                "worker1", WORKER1_NODE,
                "worker2", WORKER2_NODE
        ).entrySet()) {
            var name = e.getKey();
            var container = e.getValue();

            var ds = createDataSourceFor(container);
            POSTGRES_WORKERS.add(new PostgresWorker(
                    name, container, ds,
                    new PostgresWorkerDfs(
                            engineFactory.create(),
                            "coordinator",
                            UUID.randomUUID(),
                            config -> config.setDataSource(ds))));
        }
        POSTGRES_WORKERS = List.copyOf(POSTGRES_WORKERS);

        try(Connection connection = createDataSourceFor(COORDINATOR_NODE).getConnection();
            Statement statement = connection.createStatement()) {
            statement.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw");

            for (var workerEntry : POSTGRES_WORKERS) {
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

        for (var workerEntry : POSTGRES_WORKERS) {
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

        WORKERS = new ArrayList<>();
        for(PostgresWorker pgWorker : POSTGRES_WORKERS) {
            var workerFactory = getWorkerFactory(pgWorker, engineFactory, port);
            WORKERS.add(workerFactory.call());
        }
    }

    private static Callable<Worker> getWorkerFactory(PostgresWorker pgWorker,
                                                     SimpleScriptEngineFactory engineFactory,
                                                     int port) {
        var workerFactoryRef = new AtomicReference<Callable<Worker>>();
        var uuid = UUID.randomUUID();
        var workerFactory = (Callable<Worker>) () -> {
            final var loopTaskRef = new AtomicReference<Future<?>>();
            var worker = Worker.connect(
                    uuid,
                    pgWorker.postgresNodeName(),
                    engineFactory,
                    IO_THREAD_POOL, CPU_THREAD_POOL,
                    new PostgresWorkerDfs(
                            engineFactory.create(),
                            "coordinator",
                            uuid,
                            config -> config.setDataSource(createDataSourceFor(pgWorker.container()))),
                    new InetSocketAddress("localhost", port),
                    s -> new WorkerSocketManagerImpl(s) {
                        @Override
                        protected void doClose() throws IOException {
                            var loopTask = loopTaskRef.get();
                            if(loopTask != null)
                                loopTask.cancel(true);
                            super.doClose();
                        }
                    });
            loopTaskRef.set(IO_THREAD_POOL.submit(() -> {
                try {
                    try {
                        worker.loop();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (SimulateCrashException e) {
                        // The close call also interrupts this task, so schedule on a different thread
                        IO_THREAD_POOL.execute(() -> {
                            try {
                                worker.close();
                                WORKERS.add(workerFactoryRef.get().call());
                            } catch (Exception ex) {
                                throw new RuntimeException("Failed to restart worker", ex);
                            }
                        });
                    }
                } catch (Throwable e) {
                    LOGGER.error("Uncaught exception in worker test loop", e);
                }
            }));
            return worker;
        };
        workerFactoryRef.set(workerFactory);
        return workerFactory;
    }

    @AfterAll
    @SuppressWarnings("EmptyTryBlock")
    static void tearDown() throws Exception {
        try(var _ = Closeables.Auto.compose(Stream.concat(Stream.concat(
                                WORKERS.stream(),
                                POSTGRES_WORKERS.stream()),
                        Stream.of(COORDINATOR, COORDINATOR_NODE, NETWORK)),
                e -> new Exception("Failed to tearDown", e)
        )) {
            // I just want to close all the stuff
        } finally {
            // shutdownNow also interrupts running threads, which is needed to shut down network stuff
            CPU_THREAD_POOL.shutdownNow();
            IO_THREAD_POOL.shutdownNow();
        }
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    void wordCount() throws Exception {
        doTestWordCount("wordCountTest", "word-count.js");
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void wordCountCrash() throws Exception {
        doTestWordCount("wordCountCrashTest", "word-count-simulated-crash.js");
    }

    private void doTestWordCount(String name, String scriptName) throws Exception {
        String src;
        try (InputStream is = FILE_LOADER.loadResourceAsStream(scriptName)) {
            src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        var resDfsFile = COORDINATOR.compileAndExecuteProgram(name, src);

        List<Tuple2> res;
        try(var stream = COORDINATOR_DFS.loadAll(resDfsFile)
                .sorted(Comparator.<Tuple2>comparingInt(t -> Objects
                        .requireNonNullElse((Number) t.value(), 0)
                        .intValue()
                ).reversed())
                .limit(10)) {
            res = stream.toList();
        }

        assertEquals(
                List.of(new Tuple2("the", 906.0),
                        new Tuple2("and", 723.0),
                        new Tuple2("i", 611.0),
                        new Tuple2("to", 538.0),
                        new Tuple2("of", 475.0),
                        new Tuple2("my", 456.0),
                        new Tuple2("a", 403.0),
                        new Tuple2("you", 351.0),
                        new Tuple2("that", 315.0),
                        new Tuple2("in", 279.0)),
                res);
    }
}
