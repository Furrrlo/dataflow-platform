package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.dfs.UnimplementedDfs;
import it.polimi.ds.dataflow.coordinator.js.ProgramNashornTreeVisitor;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import org.junit.jupiter.api.Test;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PageRankTest {

    @Test
    void execute() throws ScriptException, IOException {
        // https://github.com/apache/flink/blob/9cc5ab9caf368ef336599e7d48f679c8c9750f49/flink-test-utils-parent/flink-test-utils/src/main/java/org/apache/flink/test/testdata/PageRankData.java#L34-L35

        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");
        final String programFileName = "page-rank-test.js";

        Parser parser = Parser.create("--language=es6");

        String src;
        try(InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
            src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");

        var program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader, new UnimplementedDfs()).compile(engine);

        var nf = new DecimalFormat("0.000", DecimalFormatSymbols.getInstance(Locale.ROOT));
        nf.setRoundingMode(RoundingMode.FLOOR);

        assertEquals(
                List.of(
                        new Tuple2("1", "0.238"),
                        new Tuple2("2", "0.244"),
                        new Tuple2("3", "0.170"),
                        new Tuple2("4", "0.171"),
                        new Tuple2("5", "0.174")),
                program.execute().stream()
                        .sorted(Comparator.comparing(t -> t.key().toString()))
                        .map(t -> new Tuple2(t.key(), nf.format(t.value())))
                        .collect(Collectors.toList()));
    }
}
