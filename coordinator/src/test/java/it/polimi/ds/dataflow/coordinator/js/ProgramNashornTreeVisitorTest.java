package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.coordinator.dfs.UnimplementedDfs;
import it.polimi.ds.dataflow.coordinator.src.LinesSrc;
import it.polimi.ds.dataflow.js.Op;
import it.polimi.ds.dataflow.js.OpKind;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import org.junit.jupiter.api.Test;
import org.openjdk.nashorn.api.tree.CompilationUnitTree;
import org.openjdk.nashorn.api.tree.Parser;

import javax.script.ScriptException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProgramNashornTreeVisitorTest {

    @Test
    void parse() throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final String programFileName = "word-count.js";

        Parser parser = Parser.create("--language=es6");

        String src;
        try(InputStream is = fileLoader.loadResourceAsStream(programFileName)) {
            src = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");
        var program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader, new UnimplementedDfs());

        String prefix = """
                (() => {
                   \s
                    const engineVars = (() => {
                        const raw = (name) => {
                           \s
                            return undefined;
                        };
                        return {
                            number: (name) => Number(raw(name)),
                            string: (name) => String(raw(name)),
                            boolean: (name) => !!raw(name),
                        };
                    })();
                    return\s""";
        String suffix = """
                ;
                })()\
                """;

        assertEquals(
                new Program(
                        new LinesSrc(fileLoader, "kinglear.txt", 8),
                        List.of(new Op(OpKind.FLAT_MAP, prefix + """
                                        (function(line, _) {
                                                let words = /** @type {Map<string, number>} */ new Map();
                                                line.split(/(\\s+)/).forEach(function(word) {
                                                    word = word.trim();
                                                    if(word.length !== 0)
                                                        words.set(word, (words.get(word) || 0) + 1)
                                                });
                                                return words
                                            })\
                                        """ + suffix),
                                new Op(OpKind.CHANGE_KEY, prefix + "((word, _) => word.toLowerCase())" + suffix),
                                new Op(OpKind.REDUCE, prefix + """
                                        (function(word, counts) {
                                                return counts.reduce((a, b) => a + b, 0);
                                            })\
                                        """ + suffix))),
                program);
    }

    @Test
    void parseTopLevelConstants() throws IOException, ScriptException {
        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./"));
        final String programFileName = "top-level-constants.js";
        final Parser parser = Parser.create("--language=es6");

        String src = """
                const fileName = "file.txt";
                const partitions = 16;
                const dble = 8.05;
                const flag = false;
                
                engine
                    .lines(fileName, partitions)
                    .map((line) => {
                        return flag ? dble : line;
                    });
                """;
        CompilationUnitTree cut = parser.parse(programFileName, src, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");
        var program = ProgramNashornTreeVisitor.parse(src, cut, fileLoader, new UnimplementedDfs());

        assertEquals(
                new Program(
                        new LinesSrc(fileLoader, "file.txt", 16),
                        List.of(new Op(OpKind.MAP, """
                                (() => {
                                    const fileName = "file.txt";
                                    const partitions = 16;
                                    const dble = 8.05;
                                    const flag = false;
                                    const engineVars = (() => {
                                        const raw = (name) => {
                                           \s
                                            return undefined;
                                        };
                                        return {
                                            number: (name) => Number(raw(name)),
                                            string: (name) => String(raw(name)),
                                            boolean: (name) => !!raw(name),
                                        };
                                    })();
                                    return ((line) => {
                                        return flag ? dble : line;
                                    });
                                })()\
                                """))),
                program);
    }

    @Test
    void parseRunWithEnginesVar() throws IOException, ScriptException {
        final String programFileName = "top-level-constants.js";
        final Parser parser = Parser.create("--language=es6");

        String src1 = """
                engine
                    .setup(engine => {
                        return engine
                                .declareVar('fileName', "file.txt")
                                .declareVar('partitions', 16)
                                .declareVar('dble', 8.05)
                                .declareVar('flag', false);
                    })
                    .exec(() => {
                        engine.requireInput().run('src2.js');
                    });
                """;
        String src2 = """
                engine
                    .lines(engineVars.string('fileName'), engineVars.number('partitions'))
                    .map((line) => {
                        return engineVars.boolean('flag') ? engineVars.number('dble') : line;
                    });
                """;

        final WorkDirFileLoader fileLoader = new WorkDirFileLoader(Paths.get("./")) {
            @Override
            public InputStream loadResourceAsStream(String fileName) throws IOException {
                return fileName.equals("src2.js")
                        ? new ByteArrayInputStream(src2.getBytes(StandardCharsets.UTF_8))
                        : super.loadResourceAsStream(fileName);
            }
        };

        CompilationUnitTree cut = parser.parse(programFileName, src1, System.err::println);
        if (cut == null)
            throw new UnsupportedOperationException(STR."Failed to compile \{programFileName}");
        var program = ProgramNashornTreeVisitor.parse(src1, cut, fileLoader, new UnimplementedDfs());

        assertEquals(
                new Program(
                        new LinesSrc(fileLoader, "file.txt", 16),
                        List.of(new Op(OpKind.MAP, """
                                (() => {
                                   \s
                                    const engineVars = (() => {
                                        const raw = (name) => {
                                            if(name === 'fileName') return "file.txt";
                                            if(name === 'partitions') return 16;
                                            if(name === 'dble') return 8.05;
                                            if(name === 'flag') return false;
                                            return undefined;
                                        };
                                        return {
                                            number: (name) => Number(raw(name)),
                                            string: (name) => String(raw(name)),
                                            boolean: (name) => !!raw(name),
                                        };
                                    })();
                                    return ((line) => {
                                        return engineVars.boolean('flag') ? engineVars.number('dble') : line;
                                    });
                                })()\
                                """))),
                program);
    }
}