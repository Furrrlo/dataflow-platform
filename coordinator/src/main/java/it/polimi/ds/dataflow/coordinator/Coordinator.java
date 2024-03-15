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
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.random.RandomGenerator;

public class Coordinator implements Closeable {

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
                       WorkerManager workerManager,
                       CoordinatorDfs dfs) {
        this.fileLoader = fileLoader;
        this.parser = parser;
        this.dfs = dfs;
        this.workerManager = workerManager;
    }

    @Override
    @SuppressWarnings("EmptyTryBlock")
    public void close() throws IOException {
        try (var _ = dfs;
             var _ = workerManager) {
            // I just want to close all of them
        }
    }

    public DfsFile compileAndExecuteProgram(String programFileName, String src) throws IOException, InterruptedException {
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

    public DfsFile executeProgram(String programFileName, Program program) throws IOException, InterruptedException {

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

        if (!(program.src() instanceof DfsSrc dfsSrc))
            throw new IllegalStateException("Only DfsSrc can be scheduled, but there's still " + program.src());

        LOGGER.info("Executing program {}...", programFileName);
        long startNanos = System.nanoTime();

        final var dfsFilesPrefix = dfsSrc.getDfsFile().name() + "_" + System.currentTimeMillis();

        var currDfsFile0 = dfsSrc.getDfsFile();
        var remainingOps = new ArrayList<>(program.ops());
        for (int step = 0; !remainingOps.isEmpty(); step++) {
            var currOps = nextOpsBatch(remainingOps);
            currOps.forEach(_ -> remainingOps.removeFirst());

            LOGGER.info("Executing step {}...", step);
            long startStepTime = System.nanoTime();

            final var currDfsFile = currDfsFile0;
            final var dstDfsFileName = dfsFilesPrefix + "_step" + step;

            int jobId = currentJobNumber.getAndIncrement();
            final List<DfsFilePartitionInfo> dstPartitions;
            try (var scope = new JobStructuredTaskScope<JobResult>(currDfsFile.partitionsNum())) {
                var remainingPartitions = new HashSet<>(currDfsFile.partitions());
                // Start by the ones that have a close worker
                currDfsFile.partitions().forEach(partition ->
                        workerManager.getCloseToDfsNode(partition.dfsNodeName())
                                .stream()
                                .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                                .ifPresent(worker -> {
                                    remainingPartitions.remove(partition);
                                    scope.fork(() -> scheduleJobPartition(
                                            jobId, worker, currOps, currDfsFile, partition, dstDfsFileName));
                                }));
                // Rest of them
                remainingPartitions.forEach(partition -> {
                    var worker = workerManager.getWorkers()
                            .stream()
                            .min(Comparator.comparingInt(WorkerClient::getCurrentScheduledJobs))
                            .orElseThrow(() -> new IllegalStateException("No nodes left to schedule stuff"));
                    scope.fork(() -> scheduleJobPartition(
                            jobId, worker, currOps, currDfsFile, partition, dstDfsFileName));
                });

                dstPartitions = scope.join().result(IOException::new).stream()
                        .map(r -> new DfsFilePartitionInfo(
                                dstDfsFileName,
                                r.result().pkt().dstDfsPartitionFileName(),
                                r.partition(),
                                r.result().worker().getDfsNodeName(),
                                false))
                        .toList();
                LOGGER.info("Executed step {} in {}",
                        step,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startStepTime));
            } finally {
                workerManager.unregisterConsumersForJob(jobId);
            }

            DfsFile dstDfsFile = dfs.createPartitionedFile(dstDfsFileName, dstPartitions);

            if (currOps.stream().anyMatch(o -> o.kind().isShuffles())) {
                LOGGER.info("Reshuffling");
                long startShuffleTime = System.nanoTime();
                dfs.reshuffle(dstDfsFile);
                LOGGER.info("Shuffled in {} millis", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startShuffleTime));
            }

            currDfsFile0 = dstDfsFile;
        }

        LOGGER.info("Executed program {} in {} millis",
                programFileName,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        return currDfsFile0;
    }

    private List<Op> nextOpsBatch(Iterable<Op> remainingOps) {
        var ops = new ArrayList<Op>();

        boolean wasShuffled = false;
        for (Op op : remainingOps) {
            if (wasShuffled && op.kind().isRequiresShuffling())
                return ops;

            ops.add(op);
            wasShuffled = wasShuffled || op.kind().isShuffles();
        }

        return ops;
    }

    private record JobResult(WorkerClient worker, JobSuccessPacket pkt) {
    }

    private PartitionResult<JobResult> scheduleJobPartition(
            int jobId,
            WorkerClient initialWorker,
            List<Op> ops,
            DfsFile srcFile,
            DfsFilePartitionInfo srcPartition,
            String dstDfsFileName
    ) throws InterruptedException {

        final Function<WorkerClient, ScheduleJobPacket> pktFactory = worker -> new ScheduleJobPacket(
                jobId,
                ops,
                srcFile.name(),
                srcFile.partitionsNum(),
                srcFile.partitions().stream()
                        .filter(p -> p.dfsNodeName().equals(worker.getDfsNodeName()))
                        .map(DfsFilePartitionInfo::partitionFileName)
                        .toList(),
                srcPartition.partition(),
                dstDfsFileName);

        try (var scope = new StructuredTaskScope.ShutdownOnSuccess<PartitionResult<JobResult>>()) {
            var numOfRunningTasks = new AtomicInteger();
            scheduleJobPartitionInScope(scope, pktFactory, initialWorker, numOfRunningTasks);

            while (true) {
                try {
                    scope.joinUntil(Instant.now().plus(STRAGGLERS_TIMEOUT_MILLIS, ChronoUnit.MILLIS));
                    break;
                } catch (TimeoutException e) {
                    // Timed out
                }

                // If there's anybody not doing any work, make him work and get the result of whoever finishes first
                findBestWorkerFor(srcPartition.dfsNodeName(), IDLE_WORKER_THRESHOLD)
                        .ifPresent(freeWorker ->
                                scheduleJobPartitionInScope(scope, pktFactory, freeWorker, numOfRunningTasks));
            }

            return Objects.requireNonNull(
                    scope.result(e -> new IllegalStateException(
                            STR."Failed to execute job \{jobId} for partition \{srcPartition.partition()}", e)),
                    STR."Scope result of job \{jobId} for partition \{srcPartition} is null");
        } finally {
            workerManager.unregisterConsumersForJob(jobId, srcPartition.partition());
        }
    }

    @SuppressWarnings("resource")
    private void scheduleJobPartitionInScope(
            StructuredTaskScope.ShutdownOnSuccess<PartitionResult<JobResult>> scope,
            Function<WorkerClient, ScheduleJobPacket> pktFactory,
            WorkerClient worker,
            AtomicInteger numOfRunningTasks
    ) {
        var pkt = pktFactory.apply(worker);

        var unschedule = worker.scheduleJob(pkt.jobId(), pkt.partition());
        numOfRunningTasks.getAndIncrement();
        scope.fork(() -> {
            // If it disconnects and reconnects, we want to make it pick it up from where it left off
            workerManager.registerReconnectConsumerFor(
                    worker.getUuid(), pkt.jobId(), pkt.partition(),
                    () -> scheduleJobPartitionInScope(scope, pktFactory, worker, numOfRunningTasks));

            Exception t0;
            try (var _ = unschedule;
                 var ctx = worker.getSocket().send(pkt, JobResultPacket.class)) {

                return switch (ctx.getPacket()) {
                    case JobSuccessPacket resPkt ->
                            new PartitionResult<>(pkt.partition(), new JobResult(worker, resPkt));
                    case JobFailurePacket(Exception ex) -> {
                        LOGGER.error("Worker {} failed to execute job {}", worker.getUuid(), pkt, new JobFailureException(ex));
                        throw new JobFailureException(ex);
                    }
                };
            } catch (InterruptedIOException | JobFailureException ex) {
                throw ex; // Either interrupted or the job failed, we are done
            } catch (IOException | UncheckedIOException ex) {
                if(LOGGER.isTraceEnabled())
                    LOGGER.error("Network error on Worker {} while executing job {}", worker.getUuid(), pkt, ex);
                else
                    LOGGER.error("Network error on Worker {} while executing job {}", worker.getUuid(), pkt);
                t0 = ex;
            } catch (Throwable t) {
                LOGGER.error("Unexpected error on Worker {} while executing job {}", worker.getUuid(), pkt, t);
                @SuppressWarnings("PMD.AvoidInstanceofChecksInCatchClause")
                boolean isException = t instanceof Exception;
                t0 = isException ? (Exception) t : new Exception(t);
            }

            final var t = t0;
            boolean reschedule = numOfRunningTasks.decrementAndGet() == 0;
            if (!reschedule)
                throw t;

            // Find someone else to do its job instead
            var maybeNewWorker = findBestWorkerFor(pkt.dfsSrcFileName(), Integer.MAX_VALUE);
            WorkerClient newWorker;
            if (maybeNewWorker.isPresent()) {
                newWorker = maybeNewWorker.get();
            } else {
                try {
                    newWorker = workerManager.waitForAnyReconnections()
                            .get(NO_NODES_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException innerEx) {
                    innerEx.addSuppressed(t);
                    throw innerEx;
                } catch (TimeoutException innerEx) {
                    // Try one last time, in case we missed a new connection between the previous
                    // findBestWorkerFor call and the waitForAnyReconnections call
                    newWorker = findBestWorkerFor(pkt.dfsSrcFileName(), Integer.MAX_VALUE).orElseThrow(() -> {
                        var newEx = new IOException("No nodes connected for more than "
                                + TimeUnit.MILLISECONDS.toSeconds(NO_NODES_TIMEOUT_MILLIS) + "s",
                                innerEx);
                        newEx.addSuppressed(t);
                        return newEx;
                    });
                }
            }

            scheduleJobPartitionInScope(scope, pktFactory, newWorker, numOfRunningTasks);
            throw new IllegalStateException("Rescheduled", t);
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

    private DfsSrc partitionFile(String dfsName, int partitionsNum, Src src) throws IOException, InterruptedException {
        var dfsFile = partitionFile(dfsName, partitionsNum);
        try (var tuples = src.loadAll()) {
            dfs.writeBatch(dfsFile, tuples.toList());
        }
        return new DfsSrc(dfs, dfsFile);
    }

    private DfsFile partitionFile(String dfsName, int partitionsNum) throws InterruptedException, IOException {
        var dfsFile = dfs.createPartitionedFilePreemptively(dfsName, partitionsNum);

        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            dfsFile.partitions().forEach(info -> {
                var closeNodes = workerManager.getCloseToDfsNode(info.dfsNodeName());
                if (closeNodes.isEmpty())
                    throw new IllegalStateException("Failed to create partitioned file, " +
                            STR."DFS node \{info.dfsNodeName()} has no connected workers");

                var closestNode = closeNodes.getFirst();
                scope.fork(() -> {
                    try (var ctx = closestNode.getSocket().send(
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

    public boolean ifJobAlreadyDone(String programFileName) {
        return dfs.findFile(programFileName.endsWith(".js")
                ? programFileName.substring(0, programFileName.length() - ".js".length())
                : programFileName).partitions().isEmpty();
    }

    public void deletePreviousJob(String programFileName) {
        dfs.deleteFile(programFileName.endsWith(".js")
                ? programFileName.substring(0, programFileName.length() - ".js".length())
                : programFileName);
    }
}
