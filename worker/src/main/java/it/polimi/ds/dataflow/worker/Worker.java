package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.dfs.CreateFileOptions;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.utils.IoFunction;
import it.polimi.ds.dataflow.utils.ThreadPools;
import it.polimi.ds.dataflow.worker.dfs.WorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public final class Worker implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private final ScriptEngine engine;
    private final ExecutorService ioThreadPool;
    private final ExecutorService cpuThreadPool;
    private final WorkerDfs dfs;
    private final WorkerSocketManager socket;

    public static Worker connect(UUID uuid,
                                 String dfsNodeName,
                                 ScriptEngine engine,
                                 ExecutorService ioThreadPool,
                                 ExecutorService cpuThreadPool,
                                 WorkerDfs dfs,
                                 InetSocketAddress addr,
                                 IoFunction<Socket, WorkerSocketManager> socketFactory) throws IOException {
        var socket = new Socket(addr.getAddress(), addr.getPort());
        try {
            var socketMngr = socketFactory.apply(socket);
            socketMngr.send(new HelloPacket(uuid, dfsNodeName, dfs.readWorkerJobs()));
            return new Worker(engine, ioThreadPool, cpuThreadPool, dfs, socketMngr);

        } catch (Throwable t) {
            try {
                socket.close();
            } catch (IOException ex) {
                t.addSuppressed(ex);
            }

            throw t;
        }
    }

    private Worker(ScriptEngine engine,
                   ExecutorService ioThreadPool,
                   ExecutorService cpuThreadPool,
                   WorkerDfs dfs,
                   WorkerSocketManager socket) {
        this.engine = engine;
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

    public void loop() throws IOException {
        while (!Thread.interrupted()) {
            var ctx0 = socket.receive(CoordinatorRequestPacket.class);
            ioThreadPool.execute(ThreadPools.giveNameToTask("[job-execution]", () -> {
                try (var ctx = ctx0) {
                    ctx.reply(switch (ctx.getPacket()) {
                        case ScheduleJobPacket pkt -> onScheduleJob(pkt);
                        case CreateFilePartitionPacket pkt -> onCreateFilePartition(pkt);
                    });
                } catch (IOException e) {
                    // If it's an unrecoverable failure, the next socket.receive() call is gonna blow up anyway
                    // No need to make everything die here, we can just log and go on
                    LOGGER.error("Failed to send reply to coordinator", e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
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
        try {
            var dfsSrcFile = dfs.findFile(pkt.dfsSrcFileName(), pkt.partitions(), pkt.dfsSrcPartitionNames());
            var restoredBackup = dfs.loadBackupInfo(pkt.jobId(), pkt.partition());

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

        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            return new JobFailurePacket(ex);
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
}
