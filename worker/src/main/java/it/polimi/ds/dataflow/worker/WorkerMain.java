package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.worker.socket.WorkerSocketManager;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.CompiledOp;
import it.polimi.ds.map_reduce.js.CompiledProgram;
import it.polimi.ds.map_reduce.js.Program;
import it.polimi.ds.map_reduce.socket.packets.HelloPacket;
import it.polimi.ds.map_reduce.socket.packets.JobResultPacket;
import it.polimi.ds.map_reduce.socket.packets.ScheduleJobPacket;
import it.polimi.ds.map_reduce.utils.ThreadPools;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SuppressWarnings("unused") //TODO: remove
public final class WorkerMain {
    private static final UUID uuid = UuidHandler.getUUID();
    private static final ExecutorService threadPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("worker-", 0).factory());

    @SuppressWarnings({"AddressSelection", "PMD.AvoidUsingHardCodedIP", "unused"})
    public static void main(String[] args) throws IOException {
        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        try (WorkerSocketManager mngr = new WorkerSocketManagerImpl(new Socket("127.0.0.1", 6666))) {
            mngr.send(new HelloPacket(uuid));

            while (!Thread.interrupted()) {
                var ctx = mngr.receive(ScheduleJobPacket.class);
                Future<?> taskJobExecution = threadPool.submit(ThreadPools.giveNameToTask("[job-execution]", () -> {
                            ScheduleJobPacket job = ctx.getPacket();
                            try {
                                List<CompiledOp> compileOps = Program.compile(engine, job.ops());
                                List<Tuple2> res = CompiledProgram.execute(compileOps, job.data().stream()).toList();

                                ctx.reply(new JobResultPacket(res));
                            } catch (IOException | ScriptException e) {
                                throw new RuntimeException(e);
                            }
                        })
                );

            }
        }

        threadPool.shutdown();
    }
}
