package it.polimi.ds.dataflow.coordinator;

import it.polimi.ds.dataflow.coordinator.dfs.PostgresCoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.properties.CoordinatorPropertiesHandler;
import it.polimi.ds.dataflow.coordinator.properties.CoordinatorPropertiesHandlerImpl;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.utils.UncheckedInterruptedException;
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
import java.util.Locale;
import java.util.Scanner;
import java.util.concurrent.Executors;


public final class CoordinatorMain {

    private CoordinatorMain() {
    }

    public static void main(String[] args) throws IOException, InterruptedException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final CoordinatorPropertiesHandler propsHndl = new CoordinatorPropertiesHandlerImpl(fileLoader);
        final Scanner in = new Scanner(System.in, System.console() != null ?
                System.console().charset() :
                StandardCharsets.UTF_8);

        var mainThread = Thread.currentThread();
        var threadPool = Executors.newVirtualThreadPerTaskExecutor();

        WorkerManager workerManager;
        try (var coordinator = new Coordinator(
                fileLoader,
                Parser.create("--language=es6"),
                threadPool,
                workerManager = WorkerManager.listen(threadPool, propsHndl.getListeningPort(), mainThread::interrupt),
                new PostgresCoordinatorDfs(
                        new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe"),
                        config -> {
                            PGSimpleDataSource ds = new PGSimpleDataSource();
                            ds.setUrl(propsHndl.getPgUrl());
                            ds.setUser(propsHndl.getPgUser());
                            ds.setPassword(propsHndl.getPgPassword());
                            config.setDataSource(ds);
                        },
                        workerManager::registerForeignServersUpdater)
        )) {
            inputLoop(fileLoader, in, coordinator, LoggerFactory.getLogger(CoordinatorMain.class));
        } finally {
            // shutdownNow also interrupts running threads, which is needed to shut down network stuff
            threadPool.shutdownNow();
        }
    }

    @SuppressFBWarnings("IMPROPER_UNICODE")
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

            //Possible alternative to this are:
            //  1) Create new job's table only if it doesn't exist already one with the same name.
            //     The problem is that we don't have a discriminant to decide whether keeping the old results or computing the newer ones
            //  2) Instead of dropping the job's table, deleting all rows in it for whatever reason.
            //  3) Allows the user to insert only different job's names without the possibility to use the older ones.
            //     In this case the problem would be the explosion of the DB content, which could be solvable by starting
            //     a "garbage collector" entity that every few seconds/minutes/... the oldest tables. (but this would increase
            //     the overall complexity)
            if (coordinator.ifJobAlreadyDone(programFileName)) {
                String response;
                do {
                    System.out.println("Job \"" + programFileName + "\" or another one with the same name already executed.\n" +
                            "Do you want to replace its results?(Y/N)");
                    response = in.nextLine().toLowerCase(Locale.getDefault());
                } while (!response.equals("n") && !response.equals("y"));
                if (response.equals("n"))
                    continue;

                coordinator.deletePreviousJob(programFileName);
            }

            String src;
            try (InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
                src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            try {
                coordinator.compileAndExecuteProgram(programFileName, src);
            } catch (InterruptedException | UncheckedInterruptedException | InterruptedIOException ex) {
                throw ex;
            } catch (Throwable t) {
                //Raised if an identical job (with the same name) has been already submitted
                //For instance, if word-count example is submitted two times, the second time
                //the execution will raise a DataAccessException due to the fact that the table
                //"word-count" already exists.
                //This exception is also raised when we start the coordinator, we give it a job but
                //there are 0 active workers.
                logger.error("Unrecoverable failure while executing job {}", programFileName, t);
            }
        }
    }
}
