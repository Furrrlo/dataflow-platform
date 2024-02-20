import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.js.*;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

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
        CompiledProgram program = ProgramNashornTreeVisitor.parse(src, cut).compile(engine);

        final List<Tuple2> res;
        try(Stream<Tuple2> s0 = program.src().loadInitial(fileLoader)) {
            var stream = s0;
            for (CompiledOp op : program.ops()) {
                if (op instanceof FilterCompiledOp filter) {
                    stream = stream.filter(filter);
                } else if (op instanceof MapCompiledOp map) {
                    stream = stream.map(map);
                } else if (op instanceof ChangeKeyCompiledOp map) {
                    stream = stream.map(map);
                } else if (op instanceof FlatMapCompiledOp flatMap) {
                    stream = stream.flatMap(t -> flatMap.apply(t)
                            .entrySet().stream()
                            .map(e -> new Tuple2(e.getKey(), e.getValue())));
                } else if (op instanceof ReduceCompiledOp reduce) {
                    //noinspection DataFlowIssue
                    stream = stream
                            .collect(Collectors.groupingBy(Tuple2::key, Collectors.toList()))
                            .entrySet()
                            .stream()
                            .map(e -> reduce.apply(e.getKey(), e.getValue().stream()
                                    .map(Tuple2::value)
                                    .collect(Collectors.toList())));
                }
            }

            res = stream.collect(Collectors.toList());
        }

        System.out.println(res.stream()
                .sorted(Comparator.<Tuple2>comparingInt(t -> ((Number) t.value()).intValue()).reversed())
                .limit(10)
                .collect(Collectors.toList()));
    }
}
