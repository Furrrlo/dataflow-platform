package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.js.CompiledOp;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.utils.ThreadPools;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class Worker implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private final UUID uuid;
    private final String dfsNodeName;
    private final ScriptEngine engine;
    private final ExecutorService ioThreadPool;
    private final ExecutorService cpuThreadPool;
    private final Dfs dfs;
    private final WorkerSocketManager socket;

    public Worker(UUID uuid,
                  String dfsNodeName,
                  ScriptEngine engine,
                  ExecutorService ioThreadPool,
                  ExecutorService cpuThreadPool,
                  Dfs dfs,
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
        try(var _ = socket; var _ = dfs; var _ = ioThreadPool) {
            // I just want to close everything
        }
    }

    public void loop() throws IOException {
        socket.send(new HelloPacket(uuid, dfsNodeName));

        while (!Thread.interrupted()) {
            var ctx0 = socket.receive(CoordinatorRequestPacket.class);
            ioThreadPool.execute(ThreadPools.giveNameToTask("[job-execution]", () -> {
                try(var ctx = ctx0) {
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

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    private JobResultPacket onScheduleJob(ScheduleJobPacket pkt) throws InterruptedException {
        try {
            return cpuThreadPool.submit(() -> {
                List<CompiledOp> compileOps = Program.compile(engine, pkt.ops());
                // TODO: execute properly and write res to dfs
                List<Tuple2> res = CompiledProgram.execute(compileOps, Stream.empty()).toList();
                return new JobResultPacket(res);
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e); // TODO: return error
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
