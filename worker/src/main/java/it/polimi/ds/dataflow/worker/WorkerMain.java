package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.worker.dfs.PostgresWorkerDfs;
import it.polimi.ds.dataflow.worker.socket.WorkerSocketManagerImpl;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.postgresql.ds.PGSimpleDataSource;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.Executors;

@SuppressFBWarnings({
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class WorkerMain {

    private WorkerMain() {
    }

    @SuppressFBWarnings("HES_LOCAL_EXECUTOR_SERVICE")
    @SuppressWarnings({"AddressSelection", "PMD.AvoidUsingHardCodedIP"})
    public static void main(String[] args) throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        Scanner in = new Scanner(System.in);

//        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
//        encryptor.setPassword("jasypt");
//        Properties prop = new EncryptableProperties(encryptor);
//        String propertiesFileName = "worker.config";
//        prop.load(fileLoader.loadResourceAsStream(propertiesFileName));


        final UUID uuid = UuidHandler.getUuid(fileLoader);
        System.out.println("Insert the coordinator dfs' name: ");
        final String dfsCoordinatorName = in.nextLine();   // TODO: get the coordinator name here somehow
        System.out.println("Insert the name of this worker dfs: ");
        final String dfsNodeName = in.nextLine();   // TODO: get the dfs node name here somehow

//        final UUID uuid;
//        if (prop.get("UUID") != null && !prop.get("UUID").equals(""))
//            uuid = UUID.fromString(prop.get("UUID").toString());
//        else {
//            uuid = UUID.randomUUID();
//            prop.setProperty("UUID", uuid.toString());
//        }

//        //Would be better if it was the coordinator sending its name to the worker after the HelloPacket, but at this point
//        //we don't have a socket connection to the coordinator yet.
//        final String dfsCoordinatorName = prop.get("DFS_COORDINATOR_NAME") != null
//                ? prop.get("DFS_COORDINATOR_NAME").toString()
//                : "";
//        //All workers in this way have the same name since the config file it's only one, this shouldn't be a problem
//        //because workers are also uniquely identified through their socket, and during the deployment we can set up different
//        //config files for each worker that we intend to use
//        final String dfsNodeName = prop.get("DFS_NODE_NAME") != null
//                ? prop.get("DFS_NODE_NAME").toString()
//                : "";


//        String[] pgServerNames = prop.get("PG_SERVER_NAMES").toString().split(";");
//        String pgUser = prop.get("PG_USER").toString();
//        String pgPassword = prop.get("PG_PASSWORD").toString();
//        String pgDatabaseName = prop.get("PG_PASSWORD").toString();
//        String coordinatorIp = prop.get("COORDINATOR_IP").toString();
//        String coordinatorPort = prop.get("COORDINATOR_PORT").toString();

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
                new PostgresWorkerDfs(engine, dfsCoordinatorName, uuid, config -> {
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
