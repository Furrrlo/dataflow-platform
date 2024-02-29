package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
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
import org.jspecify.annotations.Nullable;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;

public class Coordinator implements Closeable {

    private static final boolean RESHUFFLE_IN_WORKERS = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    private final WorkDirFileLoader fileLoader;
    private final Parser parser;
    private final CoordinatorDfs dfs;
    private final WorkerManager workerManager;

    public Coordinator(WorkDirFileLoader fileLoader,
                       Parser parser,
                       CoordinatorDfs dfs,
                       WorkerManager workerManager) {
        this.fileLoader = fileLoader;
        this.parser = parser;
        this.dfs = dfs;
        this.workerManager = workerManager;
    }

    @Override
    @SuppressWarnings("EmptyTryBlock")
    public void close() throws IOException {
        try(var _ = dfs;
            var _ = workerManager) {
            // I just want to close all of them
        }
    }

    public DfsFile compileAndExecuteProgram(String programFileName, String src) throws Exception {
        LOGGER.info("Compiling program {}...", programFileName);
        long startNanos = System.nanoTime();

        CompilationUnitTree cut = parser.parse(programFileName, src, info -> LOGGER.error(info.getMessage()));
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");

        LOGGER.info("Compiled program {} in {} millis",
                programFileName,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        return executeProgram(programFileName, ProgramNashornTreeVisitor.parse(src, cut, fileLoader, dfs));
    }

    public DfsFile executeProgram(String programFileName, Program program) throws Exception {

        if (program.src() instanceof NonPartitionedCoordinatorSrc nonPartitionedSrc) {
            LOGGER.info("Partitioning program {} source...", programFileName);
            long startNanos = System.nanoTime();

            program = program.withSrc(partitionFile(
                    programFileName.endsWith(".js")
                            ? programFileName.substring(0, programFileName.length() - ".js".length())
                            : programFileName,
                    nonPartitionedSrc.requestedPartitions(),
                    program.src()));

            LOGGER.info("Partitioned program {} in {} millis",
                    programFileName,
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }

        if(!(program.src() instanceof DfsSrc dfsSrc))
            throw new IllegalStateException("Only DfsSrc can be scheduled, but there's still " + program.src());

        LOGGER.info("Executing program {}...", programFileName);
        long startNanos = System.nanoTime();

        final var dfsFilesPrefix = dfsSrc.getDfsFile().name() + "_" + System.currentTimeMillis();

        var currDfsFile = dfsSrc.getDfsFile();
        var remainingOps = new ArrayList<>(program.ops());
        for(int step = 0; !remainingOps.isEmpty(); step++) {
            var currOps = nextOpsBatch(remainingOps);
            currOps.forEach(_ -> remainingOps.removeFirst());

            LOGGER.info("Executing step {}...", step);
            long startStepTime = System.nanoTime();

            int jobId = 0; // TODO
            DfsFile dstDfsFile = RESHUFFLE_IN_WORKERS
                    // If workers need to do the reshuffling, we need to be sure all files exist before scheduling 'cause
                    // otherwise one worker might need to write into the partition of another before he has created it
                    ? partitionFile(dfsFilesPrefix + "_step" + step, currDfsFile.partitionsNum())
                    // if we do the reshuffling, we can assume each worker will create his partition when he needs to
                    // write into it, and all partitions will be created when we get back to the coordinator to reshuffle
                    : dfs.createPartitionedFile(dfsFilesPrefix + "_step" + step, currDfsFile.partitionsNum());

            try (var scope = new JobStructuredTaskScope<@Nullable Object>(currDfsFile.partitionsNum())) {
                var remainingPartitions = new HashSet<>(currDfsFile.partitions());
                // Start by the ones that have a close worker
                currDfsFile.partitions().forEach(partition ->
                        workerManager.getCloseToDfsNode(partition.dfsNodeName())
                                .stream()
                                .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                                .ifPresent(worker -> {
                                    remainingPartitions.remove(partition);
                                    scheduleJobPartition(jobId, scope, worker, currOps, partition, dstDfsFile);
                                }));
                // Rest of them
                remainingPartitions.forEach(partition -> {
                    var worker = workerManager.getWorkers()
                            .stream()
                            .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                            .orElseThrow(() -> new IllegalStateException("No nodes left to schedule stuff"));
                    scheduleJobPartition(jobId, scope, worker, currOps, partition, dstDfsFile);
                });

                scope.join().result(IOException::new);
                LOGGER.info("Executed step {} in {}",
                        step,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startStepTime));
            }

            if(!RESHUFFLE_IN_WORKERS && currOps.stream().anyMatch(o -> o.kind().isShuffles())) {
                LOGGER.info("Reshuffling");
                long startShuffleTime = System.nanoTime();
                dfs.reshuffle(dstDfsFile);
                LOGGER.info("Shuffled in {} millis", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startShuffleTime));
            }

            currDfsFile = dstDfsFile;
        }

        LOGGER.info("Executed program {} in {} millis",
                programFileName,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        return currDfsFile;
    }

    private List<Op> nextOpsBatch(Iterable<Op> remainingOps) {
        var ops = new ArrayList<Op>();

        boolean wasShuffled = false;
        for(Op op : remainingOps) {
            if(wasShuffled && op.kind().isRequiresShuffling())
                return ops;

            ops.add(op);
            wasShuffled = wasShuffled || op.kind().isShuffles();
        }

        return ops;
    }

    @SuppressWarnings("resource")
    private void scheduleJobPartition(int jobId,
                                      JobStructuredTaskScope<@Nullable Object> scope,
                                      WorkerClient worker,
                                      List<Op> ops,
                                      DfsFilePartitionInfo partition,
                                      DfsFile resultingFile) {
        var unschedule = worker.scheduleJob();
        scope.fork(() -> {
            var pkt = new ScheduleJobPacket(jobId,
                    ops,
                    resultingFile.partitionsNum(),
                    partition.fileName(), partition.partition(),
                    resultingFile.name(),
                    RESHUFFLE_IN_WORKERS);

            try(var _ = unschedule;
                var ctx = worker.getSocket().send(pkt, JobResultPacket.class)) {
                return switch (ctx.getPacket()) {
                    case JobSuccessPacket resPkt ->
                            new JobStructuredTaskScope.PartitionResult<>(partition.partition(), resPkt);
                    // TODO: do error recovery (?)
                    case JobFailurePacket resPkt ->
                        throw resPkt.ex();
                };
            }
        });
    }

    private DfsSrc partitionFile(String dfsName, int partitionsNum, Src src) throws Exception {
        var dfsFile = partitionFile(dfsName, partitionsNum);
        try(var tuples = src.loadAll()) {
            dfs.writeBatch(dfsFile, tuples.toList());
        }
        return new DfsSrc(dfs, dfsFile);
    }

    private DfsFile partitionFile(String dfsName, int partitionsNum) throws Exception {
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

        return dfsFile;
    }
}
