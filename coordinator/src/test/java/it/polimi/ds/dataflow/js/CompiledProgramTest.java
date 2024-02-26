package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.src.LinesSrc;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import org.junit.jupiter.api.Test;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompiledProgramTest {

    @Test
    void execute() throws ScriptException, IOException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final ScriptEngine engine = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");

        CompiledProgram program = new Program(
                new LinesSrc(fileLoader, "kinglear.txt", 8),
                List.of(new Op(OpKind.FLAT_MAP, """
                                (function(line, _) {
                                        let words = /** @type {Map<string, number>} */ new Map();
                                        line.split(/(\\s+)/).forEach(function(word) {
                                            word = word.trim();
                                            if(word.length !== 0)
                                                words.set(word, (words.get(word) || 0) + 1)
                                        });
                                        return words
                                    })\
                                """),
                        new Op(OpKind.CHANGE_KEY, "((word, _) => word.toLowerCase())"),
                        new Op(OpKind.REDUCE, """
                                (function(word, counts) {
                                        return counts.reduce((a, b) => a + b, 0);
                                    })\
                                """))
        ).compile(engine);

        List<Tuple2> res = program.execute()
                .stream()
                .sorted(Comparator.<Tuple2>comparingInt(t -> Objects
                        .requireNonNullElse((Number) t.value(), 0)
                        .intValue()
                ).reversed())
                .limit(10)
                .toList();
        assertEquals(
                List.of(new Tuple2("the", 906.0),
                        new Tuple2("and", 723.0),
                        new Tuple2("i", 611.0),
                        new Tuple2("to", 538.0),
                        new Tuple2("of", 475.0),
                        new Tuple2("my", 456.0),
                        new Tuple2("a", 403.0),
                        new Tuple2("you", 351.0),
                        new Tuple2("that", 315.0),
                        new Tuple2("in", 279.0)),
                res);
    }
}