package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.JobStructuredTaskScope.PartitionResult;
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
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.random.RandomGenerator;

public class Coordinator implements Closeable {

    private static final boolean RESHUFFLE_IN_WORKERS = true;
    private static final int IDLE_WORKER_THRESHOLD = 1;
    private static final long STRAGGLERS_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long NO_NODES_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    private final WorkDirFileLoader fileLoader;
    private final Parser parser;
    private final CoordinatorDfs dfs;
    private final WorkerManager workerManager;
    private final AtomicInteger currentJobNumber = new AtomicInteger(RandomGenerator.getDefault().nextInt());

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

            int jobId = currentJobNumber.getAndIncrement();
            // TODO: since multiple workers can work on the same partition, they cannot use the same dst file
            //       so we cannot preemptively create them with a standardized name, they need to use their own
            //       and then tell the coordinator at the end. This also means that reshuffling can't be done in
            //       workers
            DfsFile dstDfsFile = RESHUFFLE_IN_WORKERS
                    // If workers need to do the reshuffling, we need to be sure all files exist before scheduling 'cause
                    // otherwise one worker might need to write into the partition of another before he has created it
                    ? partitionFile(dfsFilesPrefix + "_step" + step, currDfsFile.partitionsNum())
                    // if we do the reshuffling, we can assume each worker will create his partition when he needs to
                    // write into it, and all partitions will be created when we get back to the coordinator to reshuffle
                    : dfs.createPartitionedFile(dfsFilesPrefix + "_step" + step, currDfsFile.partitionsNum());

            try (var scope = new JobStructuredTaskScope<JobSuccessPacket>(currDfsFile.partitionsNum())) {
                var remainingPartitions = new HashSet<>(currDfsFile.partitions());
                // Start by the ones that have a close worker
                currDfsFile.partitions().forEach(partition ->
                        workerManager.getCloseToDfsNode(partition.dfsNodeName())
                                .stream()
                                .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                                .ifPresent(worker -> {
                                    remainingPartitions.remove(partition);
                                    scope.fork(() -> scheduleJobPartition(jobId, worker, currOps, partition, dstDfsFile));
                                }));
                // Rest of them
                remainingPartitions.forEach(partition -> {
                    var worker = workerManager.getWorkers()
                            .stream()
                            .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                            .orElseThrow(() -> new IllegalStateException("No nodes left to schedule stuff"));
                    scope.fork(() -> scheduleJobPartition(jobId, worker, currOps, partition, dstDfsFile));
                });

                scope.join().result(IOException::new);
                LOGGER.info("Executed step {} in {}",
                        step,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startStepTime));
            } finally {
                workerManager.unregisterConsumersForJob(jobId);
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

    private PartitionResult<JobSuccessPacket> scheduleJobPartition(
            int jobId,
            WorkerClient initialWorker,
            List<Op> ops,
            DfsFilePartitionInfo partition,
            DfsFile resultingFile
    ) throws InterruptedException {

        var pkt = new ScheduleJobPacket(jobId,
                ops,
                resultingFile.partitionsNum(),
                partition.fileName(), partition.partition(),
                resultingFile.name(),
                RESHUFFLE_IN_WORKERS);

        try(var scope = new StructuredTaskScope.ShutdownOnSuccess<PartitionResult<JobSuccessPacket>>()) {
            scheduleJobPartitionInScope(scope, pkt, initialWorker, true);

            while(true) {
                try {
                    scope.joinUntil(Instant.now().plus(STRAGGLERS_TIMEOUT_MILLIS, ChronoUnit.MILLIS));
                    break;
                } catch (TimeoutException e) {
                    // Timed out
                }

                // If there's anybody not doing any work, make him work and get the result of whoever finishes first
                findBestWorkerFor(partition.dfsNodeName(), IDLE_WORKER_THRESHOLD)
                        .ifPresent(freeWorker ->
                                scheduleJobPartitionInScope(scope, pkt, freeWorker, false));
            }

            return Objects.requireNonNull(
                    scope.result(IllegalStateException::new),
                    "Scope result for partition " + partition + " is null");
        } finally {
            workerManager.unregisterConsumersForJob(jobId, partition.partition());
        }
    }

    @SuppressWarnings("resource")
    private void scheduleJobPartitionInScope(
            StructuredTaskScope.ShutdownOnSuccess<PartitionResult<JobSuccessPacket>> scope,
            ScheduleJobPacket pkt,
            WorkerClient worker,
            boolean reschedule
    ) {
        var unschedule = worker.scheduleJob(pkt.jobId(), pkt.partition());
        scope.fork(() -> {
            // If it disconnects and reconnects, we want to make it pick it up from where it left off
            workerManager.registerReconnectConsumerFor(
                    worker.getUuid(), pkt.jobId(), pkt.partition(),
                    () -> scheduleJobPartitionInScope(scope, pkt, worker, false));

            try(var _ = unschedule;
                var ctx = worker.getSocket().send(pkt, JobResultPacket.class)) {

                return switch (ctx.getPacket()) {
                    case JobSuccessPacket resPkt -> new PartitionResult<>(pkt.partition(), resPkt);
                    case JobFailurePacket resPkt -> {
                        LOGGER.error("Worker {} failed to execute job {}", worker.getUuid(), pkt, resPkt.ex());
                        throw resPkt.ex();
                    }
                };
            } catch (InterruptedIOException ex) {
                throw ex; // Interrupted, we are done here
            } catch (IOException ex) {
                LOGGER.error("Network error on Worker {} while executing job {}", worker.getUuid(), pkt, ex);

                if(reschedule) {
                    // Find someone else to do its job instead
                    var maybeNewWorker = findBestWorkerFor(pkt.dfsSrcFileName(), Integer.MAX_VALUE);
                    WorkerClient newWorker;
                    if(maybeNewWorker.isPresent()) {
                        newWorker = maybeNewWorker.get();
                    } else {
                        try {
                            newWorker = workerManager.waitForAnyReconnections()
                                    .get(NO_NODES_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException innerEx) {
                            innerEx.addSuppressed(ex);
                            throw innerEx;
                        } catch (ExecutionException innerEx) {
                            ex.addSuppressed(innerEx);
                            throw ex;
                        } catch (TimeoutException innerEx) {
                            // Try one last time, in case we missed a new connection between the previous
                            // findBestWorkerFor call and the waitForAnyReconnections call
                            newWorker = findBestWorkerFor(pkt.dfsSrcFileName(), Integer.MAX_VALUE).orElseThrow(() -> {
                                ex.addSuppressed(new IOException("No nodes connected for more than "
                                        + TimeUnit.MILLISECONDS.toSeconds(NO_NODES_TIMEOUT_MILLIS) + "s",
                                        innerEx));
                                return ex;
                            });
                        }
                    }

                    scheduleJobPartitionInScope(scope, pkt, newWorker, true);
                }

                throw ex;
            }
        });
    }

    private Optional<WorkerClient> findBestWorkerFor(String dfsFileName, int maxJobsThreshold) {
        return workerManager.getCloseToDfsNode(dfsFileName)
                .stream()
                .filter(w -> w.getCurrentScheduledJobs() < maxJobsThreshold)
                .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                .or(() -> workerManager.getWorkers()
                        .stream()
                        .filter(w -> w.getCurrentScheduledJobs() < maxJobsThreshold)
                        .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs)));
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
