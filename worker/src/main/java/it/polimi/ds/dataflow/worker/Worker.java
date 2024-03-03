package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.dfs.CreateFileOptions;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.utils.ThreadPools;
import it.polimi.ds.dataflow.worker.dfs.WorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class Worker implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private final UUID uuid;
    private final static String DEFAULT_BACKUP_TABLE_NAME = "backups";
    private final String dfsNodeName;
    private final ScriptEngine engine;
    private final ExecutorService ioThreadPool;
    private final ExecutorService cpuThreadPool;
    private final WorkerDfs dfs;
    private final WorkerSocketManager socket;

    public Worker(UUID uuid,
                  String dfsNodeName,
                  ScriptEngine engine,
                  ExecutorService ioThreadPool,
                  ExecutorService cpuThreadPool,
                  WorkerDfs dfs,
                  WorkerSocketManager socket) {
        this.uuid = uuid;
        this.dfsNodeName = dfsNodeName;
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
        socket.send(new HelloPacket(uuid, dfsNodeName));

        //Creating a new table to store the backup information for fault tolerance
        dfs.createBackupFile(DEFAULT_BACKUP_TABLE_NAME);
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

    private JobResultPacket onScheduleJob(ScheduleJobPacket pkt) throws InterruptedException {
        try {
            var dfsSrcFile = dfs.findFile(pkt.dfsSrcFileName(), pkt.partitions());

            dfs.createFilePartition(pkt.dfsDstFileName(), pkt.partition(), CreateFileOptions.IF_NOT_EXISTS);
            var dfsDstFile = dfs.findFile(pkt.dfsDstFileName(), pkt.partitions());

            var compileOps = cpuThreadPool.submit(() -> Program.compile(engine, pkt.ops())).get();


            Integer nextBatchPtr = null;
            while (true) {
                var currentBatch = dfs.readNextBatch(dfsSrcFile, pkt.partition(), 1000, nextBatchPtr);

                //Saving backup information for fault tolerance
                dfs.updateBackupFile(DEFAULT_BACKUP_TABLE_NAME, new WorkerDfs.BackupInfo(
                        this.uuid,pkt.jobId(),pkt.partition(),currentBatch.nextBatchPtr()));

                if (currentBatch.data().isEmpty()) {
                    break;
                }

                var currentBatchData = currentBatch.data();
                var currentBatchRes = cpuThreadPool
                        .submit(() -> CompiledProgram.execute(compileOps, currentBatchData.stream()).toList())
                        .get();

                if (pkt.reshuffle())
                    dfs.writeBatch(dfsDstFile, currentBatchRes);
                else
                    dfs.writeBatchInPartition(dfsDstFile, pkt.partition(), currentBatchRes);
                nextBatchPtr = currentBatch.nextBatchPtr();
            }

            return new JobSuccessPacket();

        } catch (InterruptedException ex) {
            throw ex;
        } catch (Exception ex) {
            return new JobFailurePacket(ex);
        }
    }

    private CreateFilePartitionResultPacket onCreateFilePartition(CreateFilePartitionPacket pkt) {
        try {
            dfs.createFilePartition(pkt.fileName(), pkt.partitionNum());
            return new CreateFilePartitionSuccessPacket();
        } catch (Exception ex) {
            return new CreateFilePartitionFailurePacket(ex);
        }
    }
}
