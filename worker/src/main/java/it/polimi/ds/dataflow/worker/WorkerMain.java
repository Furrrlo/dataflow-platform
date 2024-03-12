package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;

@SuppressFBWarnings({
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressFBWarnings("HES_LOCAL_EXECUTOR_SERVICE")
    public static void main(String[] args) throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));

        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("jasypt");

        Path propertiesFile = fileLoader.resolvePath("worker.config");

        Properties prop = new EncryptableProperties(encryptor);
        try(Reader r = Files.newBufferedReader(propertiesFile, StandardCharsets.UTF_8)) {
            prop.load(r);
        }

        final UUID uuid;
        if (prop.getProperty("UUID") != null && !prop.getProperty("UUID").isEmpty()) {
            uuid = UUID.fromString(prop.getProperty("UUID"));
        } else {
            uuid = UUID.randomUUID();
            prop.setProperty("UUID", uuid.toString());
            try(Writer w = Files.newBufferedWriter(propertiesFile, StandardCharsets.UTF_8)) {
                prop.store(w, null);
            }
        }

        final String dfsCoordinatorName = prop.getProperty("DFS_COORDINATOR_NAME") != null
                ? prop.getProperty("DFS_COORDINATOR_NAME")
                : "";
        //All workers in this way have the same name since the config file it's only one, this shouldn't be a problem
        //because workers are also uniquely identified through their socket, and during the deployment we can set up different
        //config files for each worker that we intend to use
        final String dfsNodeName = prop.getProperty("DFS_NODE_NAME") != null
                ? prop.getProperty("DFS_NODE_NAME")
                : "";

        String pgUser = prop.getProperty("PG_USER");
        String pgPassword = prop.getProperty("PG_PASSWORD");
        String pgUrl = prop.getProperty("PG_URL");

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
                        prop.getProperty("COORDINATOR_IP"),
                        Integer.parseInt(prop.getProperty("COORDINATOR_PORT"))),
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
