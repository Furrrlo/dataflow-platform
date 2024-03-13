package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import it.polimi.ds.dataflow.worker.properties.WorkerPropertiesHandlerImpl;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.Executors;

public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressFBWarnings("HES_LOCAL_EXECUTOR_SERVICE")
    public static void main(String[] args) throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));

        final WorkerPropertiesHandlerImpl propsHndl = new WorkerPropertiesHandlerImpl(fileLoader);

        final String dfsCoordinatorName = propsHndl.getDfsCoordinatorName();
        final String dfsNodeName = propsHndl.getLocalDfsName();
        UUID uuid = propsHndl.getUuid();
        String pgUser = propsHndl.getPgUser();
        String pgPassword = propsHndl.getPgPassword();
        String pgUrl = propsHndl.getPgUrl();

        ScriptEngine engine;
        var ioThreadPool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
                .name("worker-io-", 0)
                .factory());
        var cpuThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), Thread.ofPlatform()
                .name("worker-cpu-", 0)
                .factory());
        try (Worker worker = Worker.connect(
                uuid,
                dfsNodeName,
                engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe"),
                ioThreadPool,
                cpuThreadPool,
                new PostgresWorkerDfs(engine, dfsCoordinatorName, uuid, config -> {
                    PGSimpleDataSource ds = new PGSimpleDataSource();
                    ds.setUrl(pgUrl);
                    ds.setUser(pgUser);
                    ds.setPassword(pgPassword);
                    config.setDataSource(ds);
                }),
                new InetSocketAddress(
                        propsHndl.getCoordinatorIp(),
                        propsHndl.getCoordinatorPort()),
                WorkerSocketManagerImpl::new
        )) {
            worker.loop();
        } finally {
            // shutdownNow also interrupts running threads, which is needed to shut down network stuff
            cpuThreadPool.shutdownNow();
            ioThreadPool.shutdownNow();
        }
    }
}
