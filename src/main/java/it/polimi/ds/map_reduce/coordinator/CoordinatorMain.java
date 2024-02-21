package it.polimi.ds.map_reduce.coordinator;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.CompiledProgram;
import it.polimi.ds.map_reduce.js.ProgramNashornTreeVisitor;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Scanner;
import java.util.stream.Collectors;

public class CoordinatorMain {
    public CoordinatorMain() {
    }

    public static void main(String args[]) throws ScriptException, IOException {

        final LocalSrcFileLoader fileLoader = new LocalSrcFileLoader(Paths.get("./"));
        Scanner in = new Scanner(System.in);

        Parser parser = Parser.create("--language=es6");
        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        while(!Thread.interrupted()) {
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
                    .sorted(Comparator.<Tuple2>comparingInt(t -> ((Number) t.value()).intValue()).reversed())
                    .limit(10)
                    .collect(Collectors.toList()));
        }
    }

}
