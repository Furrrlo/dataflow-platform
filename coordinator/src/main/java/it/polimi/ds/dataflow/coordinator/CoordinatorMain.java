package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.dfs.PostgresCoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.js.ProgramNashornTreeVisitor;
import it.polimi.ds.dataflow.coordinator.src.DfsSrc;
import it.polimi.ds.dataflow.coordinator.src.NonPartitionedCoordinatorSrc;
import it.polimi.ds.map_reduce.js.Program;
import it.polimi.ds.map_reduce.socket.packets.CreateFilePartitionPacket;
import it.polimi.ds.map_reduce.src.WorkDirFileLoader;
import it.polimi.ds.map_reduce.src.Src;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.StructuredTaskScope;

@SuppressFBWarnings({
        "LO_SUSPECT_LOG_PARAMETER", // In order to avoid static initialization before main is called
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class CoordinatorMain {

    private final WorkDirFileLoader fileLoader;
    private final Scanner in;
    private final Parser parser;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    @SuppressFBWarnings("FCBL_FIELD_COULD_BE_LOCAL")
    private final ExecutorService threadPool;
    private final CoordinatorDfs dfs;
    private final WorkerManager workerManager;
    private final Logger logger;

    public CoordinatorMain(WorkDirFileLoader fileLoader,
                           Scanner in,
                           Parser parser,
                           ExecutorService threadPool,
                           CoordinatorDfs dfs,
                           WorkerManager workerManager,
                           Logger logger) {
        this.fileLoader = fileLoader;
        this.in = in;
        this.parser = parser;
        this.threadPool = threadPool;
        this.dfs = dfs;
        this.workerManager = workerManager;
        this.logger = logger;
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public static void main(String[] args) throws IOException, InterruptedException {

        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final Scanner in = new Scanner(System.in, System.console() != null ?
                System.console().charset() :
                StandardCharsets.UTF_8);
        final Parser parser = Parser.create("--language=es6");

        try (
                ExecutorService threadPool = Executors.newVirtualThreadPerTaskExecutor();
                CoordinatorDfs dfs = new PostgresCoordinatorDfs(config -> {
                    PGSimpleDataSource ds = new PGSimpleDataSource();
                    ds.setServerNames(new String[]{"localhost"});
                    ds.setUser("postgres");
                    ds.setPassword("password");
                    ds.setDatabaseName("postgres");
                    config.setDataSource(ds);
                });
                WorkerManager workerManager = WorkerManager.listen(threadPool, 6666)
        ) {
            new CoordinatorMain(
                    fileLoader,
                    in,
                    parser,
                    threadPool,
                    dfs,
                    workerManager,
                    LoggerFactory.getLogger(CoordinatorMain.class)
            ).inputLoop();
        }
    }

    private void inputLoop() throws IOException, InterruptedException {
        while (!Thread.interrupted()) {
            logger.info("Insert the program file path: ");
            String programFileName = in.nextLine();

            if (!fileLoader.resourceExists(programFileName)) {
                logger.error("File not found");
                continue;
            }

            String src;
            try (InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
                src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            try {
                executeProgram(programFileName, src);
            } catch (InterruptedException | InterruptedIOException ex) {
                throw ex;
            } catch (Throwable t) {
                logger.error("Unrecoverable failure while executing job {}", programFileName, t);
            }
        }
    }

    @SuppressFBWarnings("DLS_DEAD_LOCAL_STORE") // TODO: remove
    private void executeProgram(String programFileName, String src) throws Exception {

        CompilationUnitTree cut = parser.parse(programFileName, src, info -> logger.error(info.getMessage()));
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");
        Program program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader, dfs);

        if (program.src() instanceof NonPartitionedCoordinatorSrc nonPartitionedSrc)
            program = program.withSrc(partitionFile(
                    programFileName.endsWith(".js")
                            ? programFileName.substring(0, programFileName.length() - ".js".length())
                            : programFileName,
                    nonPartitionedSrc.requestedPartitions(),
                    program.src()));

        // TODO: execute program here on the distributed data
    }

    private Src partitionFile(String dfsName, int partitionsNum, Src src) throws Exception {
        var dfsFile = dfs.createPartitionedFile(dfsName, partitionsNum);

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            dfsFile.partitions().forEach(info -> {
                var closeNodes = workerManager.getCloseToDfsNode(info.dfsNodeName());
                if(closeNodes.isEmpty())
                    throw new IllegalStateException("Failed to create partitioned file, " +
                            STR."DFS node \{info.dfsNodeName()} has no connected workers");

                var closestNode = closeNodes.getFirst();
                scope.fork(() -> {
                    closestNode.socket().send(new CreateFilePartitionPacket(info.fileName(), info.partition()));
                    return null;
                });
            });

            scope.join().throwIfFailed();
        }

        dfs.write(dfsFile, src.loadAll());
        return new DfsSrc(dfs, dfsFile);
    }
}
