package it.polimi.ds.map_reduce.coordinator;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.CompiledProgram;
import it.polimi.ds.map_reduce.js.ProgramNashornTreeVisitor;
import it.polimi.ds.map_reduce.socket.packets.HelloPacket;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.jetbrains.annotations.VisibleForTesting;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public final class CoordinatorMain {

    private CoordinatorMain() {
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public static void main(String[] args) throws ScriptException, IOException {

        final LocalSrcFileLoader fileLoader = new LocalSrcFileLoader(Paths.get("./"));
        final Scanner in = new Scanner(System.in, System.console() != null ?
                System.console().charset() :
                StandardCharsets.UTF_8);

        Parser parser = Parser.create("--language=es6");
        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        while (!Thread.interrupted()) {
            System.out.println("Insert the program file path: ");
            String programFileName = in.nextLine();

            if (!fileLoader.exists(programFileName)) {
                System.out.println("File not found");
                continue;
            }

            String src;
            try (InputStream is = fileLoader.loadAsStream(programFileName)) {
                src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
            if (cut == null)
                throw new UnsupportedOperationException("Failed to compile " + programFileName);
            CompiledProgram program = ProgramNashornTreeVisitor.parse(src, cut).compile(engine);

            System.out.println(program.execute(fileLoader)
                    .stream()
                    .sorted(Comparator.<Tuple2>comparingInt(t -> Objects
                            .requireNonNullElse((Number) t.value(), 0)
                            .intValue()
                    ).reversed())
                    .limit(10)
                    .collect(Collectors.toList()));


            connectionTest();

        }
    }

    @VisibleForTesting
    static void connectionTest() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(6666)) {

            while (!Thread.interrupted()) {
                final Socket socket = serverSocket.accept();
                CoordinatorSocketManager mngr = new CoordinatorSocketManagerImpl(Executors.newThreadPerTaskExecutor(Thread.ofVirtual().factory()), socket);
                var helloPckCtx = mngr.receive(HelloPacket.class);
                System.out.println("Received hello packet");
                helloPckCtx.ack();
                System.out.println("Sent response hello packet");
                System.out.println("OK");
            }
        }
    }

}
