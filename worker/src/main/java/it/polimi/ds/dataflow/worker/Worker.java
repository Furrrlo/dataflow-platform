package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.dfs.CreateFileOptions;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.utils.IoFunction;
import it.polimi.ds.dataflow.utils.SimpleScriptEngineFactory;
import it.polimi.ds.dataflow.utils.ThreadPools;
import it.polimi.ds.dataflow.worker.dfs.WorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import org.jspecify.annotations.Nullable;
import org.openjdk.nashorn.api.scripting.NashornException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public final class Worker implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private final SimpleScriptEngineFactory engineFactory;
    private final ExecutorService ioThreadPool;
    private final ExecutorService cpuThreadPool;
    private final WorkerDfs dfs;
    private final WorkerSocketManager socket;

    private final Set<ScheduledJob> currentScheduledJobs = ConcurrentHashMap.newKeySet();
    private volatile @Nullable SimulateCrashException simulatedCrash;

    public static Worker connect(UUID uuid,
                                 String dfsNodeName,
                                 SimpleScriptEngineFactory engineFactory0,
                                 ExecutorService ioThreadPool,
                                 ExecutorService cpuThreadPool,
                                 WorkerDfs dfs,
                                 InetSocketAddress addr,
                                 IoFunction<Socket, WorkerSocketManager> socketFactory) throws IOException {
        SimpleScriptEngineFactory engineFactory = () -> {
            var engine = engineFactory0.create();
            engine.put("simulateCrash", engine.eval(STR."""
                    (function() {
                        let SimulateCrashException = Java.type("\{SimulateCrashException.class.getName()}");
                        return function() { throw new SimulateCrashException(); };
                    })();
                    """));
            return engine;
        };

        var socket = new Socket(addr.getAddress(), addr.getPort());
        try {
            var socketMngr = socketFactory.apply(socket);
            socket.setSoTimeout(PingPacket.TIMEOUT_MILLIS);
            socketMngr.send(new HelloPacket(uuid, dfsNodeName, dfs.readWorkerJobs()));
            return new Worker(engineFactory, ioThreadPool, cpuThreadPool, dfs, socketMngr);

        } catch (Throwable t) {
            try {
                socket.close();
            } catch (IOException ex) {
                t.addSuppressed(ex);
            }

            throw t;
        }
    }

    private Worker(SimpleScriptEngineFactory engineFactory,
                   ExecutorService ioThreadPool,
                   ExecutorService cpuThreadPool,
                   WorkerDfs dfs,
                   WorkerSocketManager socket) {
        this.engineFactory = engineFactory;
        this.ioThreadPool = ioThreadPool;
        this.cpuThreadPool = cpuThreadPool;
        this.dfs = dfs;
        this.socket = socket;
    }

    @Override
    @SuppressWarnings("EmptyTryBlock")
    public void close() throws IOException {
        try (var _ = dfs;
             var _ = socket) {
            // I just want to close everything
        }
    }

    public void loop() throws IOException, InterruptedException {
        var loopThread = Thread.currentThread();
        SimulateCrashException simulatedCrash;
        try {
            while (!Thread.interrupted()) {
                var ctx0 = socket.receive(CoordinatorRequestPacket.class);

                ioThreadPool.execute(ThreadPools.giveNameToTask("[job-execution]", () -> {
                    try (var ctx = ctx0) {
                        ctx.reply(switch (ctx.getPacket()) {
                            case ScheduleJobPacket pkt -> onScheduleJob(pkt);
                            case CreateFilePartitionPacket pkt -> onCreateFilePartition(pkt);
                            case PingPacket _ -> new PongPacket();
                        });
                    } catch (IOException e) {
                        // If it's an unrecoverable failure, the next socket.receive() call is gonna blow up anyway
                        // No need to make everything die here, we can just log and go on
                        LOGGER.error("Failed to send reply to coordinator", e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (SimulateCrashException e) {
                        Thread.currentThread().interrupt();
                        this.simulatedCrash = e;
                        loopThread.interrupt();
                    }
                }));
            }

            simulatedCrash = this.simulatedCrash;
        } catch (InterruptedIOException ex) {
            simulatedCrash = this.simulatedCrash;
            if(simulatedCrash == null)
                throw (InterruptedException) new InterruptedException().initCause(ex);
        }

        this.simulatedCrash = null;
        if(simulatedCrash != null) {
            @SuppressWarnings("unused") var unused = Thread.interrupted(); // Clear interrupt flag
            throw new SimulateCrashException(simulatedCrash);
        }
    }

    /**
     * Handles the reception of a {@link ScheduleJobPacket} by creating a new file partition in the
     * DFS if it doesn't exist yet. Execute the operation on batches of the partition's data specified
     * in the packet.
     * Write in the appropriate partitions according to the {@link ScheduleJobPacket#partition()}
     *
     * @param pkt packet sent by the coordinator and containing the job to be executed
     * @return a response packet which attests whether the job was executed successfully or not
     * @throws InterruptedException if the current thread was interrupted while waiting
     */
    private JobResultPacket onScheduleJob(ScheduleJobPacket pkt) throws InterruptedException {
        var scheduledJob = new ScheduledJob(pkt.jobId(), pkt.partition());
        boolean alreadyScheduled = !currentScheduledJobs.add(scheduledJob);
        if (alreadyScheduled)
            return new JobFailurePacket(new IllegalStateException(
                    "Job " + pkt.jobId() + " for partition " + pkt.partition() +
                            " was already scheduled on this node"));

        try {
            var engine = engineFactory.create();
            var dfsSrcFile = dfs.findFile(pkt.dfsSrcFileName(), pkt.partitions(), pkt.dfsSrcPartitionNames());
            var restoredBackup = dfs.loadBackupInfo(pkt.jobId(), pkt.partition(), pkt.dfsDstFileName());

            // Create the partition in which we are going to put the results
            var dfsDstFilePartition = restoredBackup != null ?
                    restoredBackup.dstFilePartition() :
                    dfs.createTempFilePartition(
                            pkt.dfsDstFileName(),
                            pkt.partition(),
                            CreateFileOptions.IF_NOT_EXISTS);

            // Initialize backup information
            if(restoredBackup == null)
                dfs.writeBackupInfo(pkt.jobId(), pkt.partition(), dfsDstFilePartition, null);

            var compiledOps = cpuThreadPool.submit(() -> Program.compile(engine, pkt.ops())).get();

            Integer nextBatchPtr = restoredBackup != null ? restoredBackup.nextBatchPtr() : null;
            while (true) {
                var currentBatch = dfs.readNextBatch(dfsSrcFile, pkt.partition(), 1000, nextBatchPtr);

                if (currentBatch.data().isEmpty()) {
                    break;
                }

                var currentBatchData = currentBatch.data();
                var currentBatchRes = cpuThreadPool
                        .submit(() -> CompiledProgram.execute(compiledOps, currentBatchData.stream()).toList())
                        .get();

                dfs.writeBatchInPartitionAndBackup(
                        pkt.jobId(), pkt.partition(),
                        dfsDstFilePartition,
                        currentBatchRes, currentBatch.nextBatchPtr());
                nextBatchPtr = currentBatch.nextBatchPtr();
            }

            return new JobSuccessPacket(dfsDstFilePartition.partitionFileName());

        } catch (InterruptedException | SimulateCrashException ex) {
            throw ex;
        } catch (ExecutionException ex) {
            if(ex.getCause() instanceof NashornException jsException &&
                    jsException.getCause() instanceof SimulateCrashException simulateEx)
                throw simulateEx;

            return new JobFailurePacket(ex);
        } catch (Exception ex) {
            return new JobFailurePacket(ex);
        } finally {
            currentScheduledJobs.remove(scheduledJob);
        }
    }

    /**
     * @param pkt packet with the info of the partition that must be created
     * @return a successful packet if the partition has been correctly created, a failure packet otherwise
     */
    private CreateFilePartitionResultPacket onCreateFilePartition(CreateFilePartitionPacket pkt) {
        try {
            dfs.createFilePartition(pkt.fileName(), pkt.partitionNum());
            return new CreateFilePartitionSuccessPacket();
        } catch (Exception ex) {
            return new CreateFilePartitionFailurePacket(ex);
        }
    }

    private record ScheduledJob(int jobId, int partition) {
    }
}
