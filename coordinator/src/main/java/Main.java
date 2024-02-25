import it.polimi.ds.dataflow.coordinator.js.ProgramNashornTreeVisitor;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.CompiledProgram;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings("DefaultPackage")
public final class Main {

    private Main() {
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public static void main(String[] args) throws IOException, ScriptException {
        final LocalSrcFileLoader fileLoader = new LocalSrcFileLoader(Paths.get("./"));
        final String programFileName = "word-count.js";

        Parser parser = Parser.create("--language=es6");
        ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        String src;
        try(InputStream is = fileLoader.loadAsStream(programFileName)) {
            src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException("Failed to compile " + programFileName);
        CompiledProgram program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader).compile(engine);

        System.out.println(program.execute()
                .stream()
                .sorted(Comparator.<Tuple2>comparingInt(t -> Objects
                        .requireNonNullElse((Number) t.value(), 0)
                        .intValue()
                ).reversed())
                .limit(10)
                .collect(Collectors.toList()));
    }
}
