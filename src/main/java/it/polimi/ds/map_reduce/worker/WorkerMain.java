package it.polimi.ds.map_reduce.worker;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.CompiledOp;
import it.polimi.ds.map_reduce.js.CompiledProgram;
import it.polimi.ds.map_reduce.js.Program;
import it.polimi.ds.map_reduce.socket.packets.JobResultPacket;
import it.polimi.ds.map_reduce.socket.packets.ScheduleJobPacket;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressWarnings({ "AddressSelection", "PMD.AvoidUsingHardCodedIP" })
    public static void main(String[] args) throws IOException {
        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        try (WorkerSocketManager mngr = new WorkerSocketManagerImpl(new Socket("127.0.0.1", 6666))) {

            while (!Thread.interrupted()) {
                var ctx = mngr.receive(ScheduleJobPacket.class);

                new Thread(() -> {
                    ScheduleJobPacket job = ctx.getPacket();

                    try {
                        List<CompiledOp> compileOps = Program.compile(engine, job.ops());
                        List<Tuple2> res = CompiledProgram.execute(compileOps, job.data().stream()).toList();
                        ctx.reply(new JobResultPacket(res));
                    } catch (IOException | ScriptException e) {
                        throw new RuntimeException(e);
                    }
                }).start();
            }
        }
    }
}
