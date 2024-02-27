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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.StructuredTaskScope;

public class Coordinator implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    private final WorkDirFileLoader fileLoader;
    private final Parser parser;
    private final ExecutorService threadPool;
    private final CoordinatorDfs dfs;
    private final WorkerManager workerManager;

    public Coordinator(WorkDirFileLoader fileLoader,
                       Parser parser,
                       ExecutorService threadPool,
                       CoordinatorDfs dfs,
                       WorkerManager workerManager) {
        this.fileLoader = fileLoader;
        this.parser = parser;
        this.threadPool = threadPool;
        this.dfs = dfs;
        this.workerManager = workerManager;
    }

    @Override
    @SuppressWarnings("EmptyTryBlock")
    public void close() throws IOException {
        try(var _ = workerManager; var _ = dfs; var _ = threadPool) {
            // I just want to close all of them
        }
    }

    public void compileAndExecuteProgram(String programFileName, String src) throws Exception {
        CompilationUnitTree cut = parser.parse(programFileName, src, info -> LOGGER.error(info.getMessage()));
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");

        executeProgram(programFileName, ProgramNashornTreeVisitor.parse(src, cut, fileLoader, dfs));
    }

    public void executeProgram(String programFileName, Program program) throws Exception {

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
                                      WorkerClient worker,
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
