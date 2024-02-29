package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Executors;

@SuppressFBWarnings({
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressWarnings({"AddressSelection", "PMD.AvoidUsingHardCodedIP"})
    public static void main(String[] args) throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));

        final UUID uuid = UuidHandler.getUuid(fileLoader);
        final String dfsCoordinatorName = ""; // TODO: get the coordinator name here somehow
        final String dfsNodeName = ""; // TODO: get the dfs node name here somehow

        ScriptEngine engine;
        var ioThreadPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("worker-io-", 0)
                .factory());
        var cpuThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), Thread.ofPlatform()
                .name("worker-cpu-", 0)
                .factory());
        try (Worker worker = new Worker(
                uuid,
                dfsNodeName,
                engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe"),
                ioThreadPool,
                cpuThreadPool,
                new PostgresDfs(engine, dfsCoordinatorName, config -> {
                    PGSimpleDataSource ds = new PGSimpleDataSource();
                    ds.setServerNames(new String[]{"localhost"});
                    ds.setUser("postgres");
                    ds.setPassword("password");
                    ds.setDatabaseName("postgres");
                    config.setDataSource(ds);
                }),
                new WorkerSocketManagerImpl(new Socket("127.0.0.1", 6666))
        )) {
            worker.loop();
        } finally {
            // shutdownNow also interrupts running threads, which is needed to shut down network stuff
            cpuThreadPool.shutdownNow();
            ioThreadPool.shutdownNow();
        }
    }
}
