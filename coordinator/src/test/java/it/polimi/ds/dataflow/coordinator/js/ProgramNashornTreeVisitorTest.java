package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.coordinator.src.LinesSrc;
import it.polimi.ds.map_reduce.js.Op;
import it.polimi.ds.map_reduce.js.OpKind;
import it.polimi.ds.map_reduce.js.Program;
import it.polimi.ds.map_reduce.src.LocalFileLoader;
import org.junit.jupiter.api.Test;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProgramNashornTreeVisitorTest {

    @Test
    void parse() throws IOException {
        final LocalFileLoader fileLoader = new LocalFileLoader(Paths.get("./"));
        final String programFileName = "word-count.js";

        Parser parser = Parser.create("--language=es6");

        String src;
        try(InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
            src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");
        var program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader, null);

        assertEquals(
                new Program(
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
                                        """))),
                program);
    }
}