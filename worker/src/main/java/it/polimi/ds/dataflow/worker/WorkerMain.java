package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.js.CompiledOp;
import it.polimi.ds.dataflow.js.CompiledProgram;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.socket.packets.HelloPacket;
import it.polimi.ds.dataflow.socket.packets.JobResultPacket;
import it.polimi.ds.dataflow.socket.packets.ScheduleJobPacket;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.ThreadPools;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

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

public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressWarnings({"AddressSelection", "PMD.AvoidUsingHardCodedIP"})
    public static void main(String[] args) throws IOException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        final UUID uuid = UuidHandler.getUuid(fileLoader);
        try (ExecutorService threadPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("worker-", 0)
                .factory());
             WorkerSocketManager mngr = new WorkerSocketManagerImpl(new Socket("127.0.0.1", 6666))) {

            mngr.send(new HelloPacket(uuid));

            while (!Thread.interrupted()) {
                // TODO: receive CreateFilePartitionPacket
                var ctx = mngr.receive(ScheduleJobPacket.class);
                threadPool.execute(ThreadPools.giveNameToTask("[job-execution]", () -> {
                    ScheduleJobPacket job = ctx.getPacket();
                    try {
                        List<CompiledOp> compileOps = Program.compile(engine, job.ops());
                        // TODO: execute properly and write res to dfs
                        List<Tuple2> res = CompiledProgram.execute(compileOps, Stream.empty()).toList();

                        ctx.reply(new JobResultPacket(res));
                    } catch (IOException | ScriptException e) {
                        throw new RuntimeException(e); // TODO: handle errors
                    }
                }));
            }
        }
    }
}
