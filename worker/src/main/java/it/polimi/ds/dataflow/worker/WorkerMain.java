package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.js.CompiledOp;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.*;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.utils.ThreadPools;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@SuppressFBWarnings({
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressWarnings({"AddressSelection", "PMD.AvoidUsingHardCodedIP"})
    public static void main(String[] args) throws IOException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        final UUID uuid = UuidHandler.getUuid(fileLoader);
        final String dfsCoordinatorName = ""; // TODO: get the coordinator name here somehow
        final String dfsNodeName = ""; // TODO: get the dfs node name here somehow

        try (ExecutorService threadPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("worker-", 0)
                .factory());
             Dfs dfs = new PostgresDfs(dfsCoordinatorName, config -> {
                 PGSimpleDataSource ds = new PGSimpleDataSource();
                 ds.setServerNames(new String[]{"localhost"});
                 ds.setUser("postgres");
                 ds.setPassword("password");
                 ds.setDatabaseName("postgres");
                 config.setDataSource(ds);
             });
             WorkerSocketManager mngr = new WorkerSocketManagerImpl(new Socket("127.0.0.1", 6666))) {
            mngr.send(new HelloPacket(uuid, dfsNodeName));

            while (!Thread.interrupted()) {
                var ctx0 = mngr.receive(CoordinatorRequestPacket.class);
                threadPool.execute(ThreadPools.giveNameToTask("[job-execution]", () -> {
                    try(var ctx = ctx0) {
                        ctx.reply(switch (ctx.getPacket()) {
                            case ScheduleJobPacket pkt -> onScheduleJob(dfs, engine, pkt);
                            case CreateFilePartitionPacket pkt -> onCreateFilePartition(dfs, pkt);
                        });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
        }
    }

    private static JobResultPacket onScheduleJob(@SuppressWarnings("unused") Dfs dfs,
                                                 ScriptEngine engine,
                                                 ScheduleJobPacket pkt) {
        try {
            List<CompiledOp> compileOps = Program.compile(engine, pkt.ops());
            // TODO: execute properly and write res to dfs
            List<Tuple2> res = CompiledProgram.execute(compileOps, Stream.empty()).toList();
            return new JobResultPacket(res);
        } catch (ScriptException e) {
            throw new RuntimeException(e); // TODO: handle errors
        }
    }

    private static CreateFilePartitionResultPacket onCreateFilePartition(Dfs dfs, CreateFilePartitionPacket pkt) {
        try {
            dfs.createFilePartition(pkt.fileName(), pkt.partitionNum());
            return new CreateFilePartitionSuccessPacket();
        } catch (Exception ex) {
            return new CreateFilePartitionFailurePacket(ex);
        }
    }
}
