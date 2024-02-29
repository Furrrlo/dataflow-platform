package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.dfs.PostgresCoordinatorDfs;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.Parser;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.Executors;

@SuppressFBWarnings({
        "HARD_CODE_PASSWORD" // TODO: load the credentials from somewhere
})
public final class CoordinatorMain {

    private CoordinatorMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, ScriptException {

        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final Scanner in = new Scanner(System.in, System.console() != null ?
                System.console().charset() :
                StandardCharsets.UTF_8);

        var threadPool = Executors.newVirtualThreadPerTaskExecutor();
        try (var coordinator = new Coordinator(
                fileLoader,
                Parser.create("--language=es6"),
                new PostgresCoordinatorDfs(
                        new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe"),
                        config -> {
                            PGSimpleDataSource ds = new PGSimpleDataSource();
                            ds.setServerNames(new String[]{"localhost"});
                            ds.setUser("postgres");
                            ds.setPassword("password");
                            ds.setDatabaseName("postgres");
                            config.setDataSource(ds);
                        }),
                WorkerManager.listen(threadPool, 6666))
        ) {
            inputLoop(fileLoader, in, coordinator, LoggerFactory.getLogger(CoordinatorMain.class));
        } finally {
            // shutdownNow also interrupts running threads, which is needed to shut down network stuff
            threadPool.shutdownNow();
        }
    }

    private static void inputLoop(
            WorkDirFileLoader fileLoader,
            Scanner in,
            Coordinator coordinator,
            Logger logger
    ) throws IOException, InterruptedException {

        while (!Thread.interrupted()) {
            logger.info("Insert the program file path: ");
            String programFileName = in.nextLine();

            if (!fileLoader.resourceExists(programFileName)) {
                logger.error("File not found");
                continue;
            }

            String src;
            try (InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
                src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            try {
                coordinator.compileAndExecuteProgram(programFileName, src);
            } catch (InterruptedException | InterruptedIOException ex) {
                throw ex;
            } catch (Throwable t) {
                logger.error("Unrecoverable failure while executing job {}", programFileName, t);
            }
        }
    }
}
