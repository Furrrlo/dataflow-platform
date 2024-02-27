package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.dfs.PostgresCoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.js.ProgramNashornTreeVisitor;
import it.polimi.ds.dataflow.coordinator.src.DfsSrc;
import it.polimi.ds.dataflow.coordinator.src.NonPartitionedCoordinatorSrc;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.js.Op;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.src.Src;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;
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
import java.util.*;
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

        if(!(program.src() instanceof DfsSrc dfsSrc))
            throw new IllegalStateException("Only DfsSrc can be scheduled, but there's still " + program.src());

        final var dfsFilesPrefix = dfsSrc.getDfsFile().name() + "_" + System.currentTimeMillis();

        var currDfsFile = dfsSrc.getDfsFile();
        var remainingOps = new ArrayList<>(program.ops());
        for(int step = 0; !remainingOps.isEmpty(); step++) {
            var currOps = nextOpsBatch(remainingOps);
            currOps.forEach(_ -> remainingOps.removeFirst());

            int jobId = 0; // TODO
            DfsFile dstDfsFile = dfs.createPartitionedFile(
                    dfsFilesPrefix + "_step" + step,
                    currDfsFile.partitionsNum());

            try (var scope = new JobStructuredTaskScope<@Nullable Object>(currDfsFile.partitionsNum())) {
                var remainingPartitions = new HashSet<>(currDfsFile.partitions());
                // Start by the ones that have a close worker
                currDfsFile.partitions().forEach(partition ->
                        workerManager.getCloseToDfsNode(partition.dfsNodeName())
                                .stream()
                                .min(Comparator.comparingInt(Worker::getCurrentScheduledJobs))
                                .ifPresent(worker -> {
                                    remainingPartitions.remove(partition);
                                    scheduleJobPartition(jobId, scope, worker, currOps, partition, dstDfsFile);
                                }));
                // Rest of them
                remainingPartitions.forEach(partition -> {
                    var worker = workerManager.getWorkers()
                            .stream()
                            .min(Comparator.comparingInt(Worker::getCurrentScheduledJobs))
                            .orElseThrow(() -> new IllegalStateException("No nodes left to schedule stuff"));
                    scheduleJobPartition(jobId, scope, worker, currOps, partition, dstDfsFile);
                });

                scope.join().result(IOException::new);
            }

//            if(currOps.stream().anyMatch(o -> o.kind().isShuffles())) {
//                // TODO: shuffle
//            }

            currDfsFile = dstDfsFile;
        }
    }

    private List<Op> nextOpsBatch(Iterable<Op> remainingOps) {
        var ops = new ArrayList<Op>();

        boolean wasShuffled = false;
        for(Op op : remainingOps) {
            if(wasShuffled && op.kind().isRequiresShuffling())
                return ops;

            wasShuffled = wasShuffled || op.kind().isShuffles();
        }

        return ops;
    }

    @SuppressWarnings("resource")
    private void scheduleJobPartition(int jobId,
                                      JobStructuredTaskScope<@Nullable Object> scope,
                                      Worker worker,
                                      List<Op> ops,
                                      DfsFilePartitionInfo partition,
                                      DfsFile resultingFile) {
        var unschedule = worker.scheduleJob();
        scope.fork(() -> {
            var pkt = new ScheduleJobPacket(jobId,
                    ops,
                    partition.fileName(), partition.partition(),
                    resultingFile.name());

            try(var _ = unschedule;
                var ctx = worker.getSocket().send(pkt, JobResultPacket.class)) {

                return new JobStructuredTaskScope.PartitionResult<>(partition.partition(), ctx.getPacket());
            }
        });
    }

    private DfsSrc partitionFile(String dfsName, int partitionsNum, Src src) throws Exception {
        var dfsFile = dfs.createPartitionedFile(dfsName, partitionsNum);

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            dfsFile.partitions().forEach(info -> {
                var closeNodes = workerManager.getCloseToDfsNode(info.dfsNodeName());
                if(closeNodes.isEmpty())
                    throw new IllegalStateException("Failed to create partitioned file, " +
                            STR."DFS node \{info.dfsNodeName()} has no connected workers");

                var closestNode = closeNodes.getFirst();
                scope.fork(() -> {
                    try(var ctx = closestNode.getSocket().send(
                            new CreateFilePartitionPacket(info.fileName(), info.partition()),
                            CreateFilePartitionResultPacket.class
                    )) {
                        return switch (ctx.getPacket()) {
                            case CreateFilePartitionSuccessPacket _ -> null;
                            case CreateFilePartitionFailurePacket pkt -> throw pkt.exception();
                        };
                    }
                });
            });

            scope.join().throwIfFailed(ex -> new IOException(
                    STR."Failed to partition file \{dfsName} in \{partitionsNum}",
                    ex));
        }

        dfs.write(dfsFile, src.loadAll());
        return new DfsSrc(dfs, dfsFile);
    }
}
