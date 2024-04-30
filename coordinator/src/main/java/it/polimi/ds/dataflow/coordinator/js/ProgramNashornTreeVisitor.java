package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.src.*;
import it.polimi.ds.dataflow.js.Op;
import it.polimi.ds.dataflow.js.OpKind;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.src.Src;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.FastIllegalStateException;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.tree.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings({
        "ClassEscapesDefinedScope" // There's no way to get an instance of this class, it can't escape
})
public final class ProgramNashornTreeVisitor extends ThrowingNashornTreeVisitor<Program, ProgramNashornTreeVisitor.Ctx> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProgramNashornTreeVisitor.class);
    private static final ProgramNashornTreeVisitor INSTANCE = new ProgramNashornTreeVisitor();

    @SuppressWarnings({
            "PMD.PreserveStackTrace", // Done on purpose to let checked exception through
            "PMD.IdenticalCatchBranches", // The two branches throw different checked exceptions, cannot be merged
    })
    @SuppressFBWarnings("LEST_LOST_EXCEPTION_STACK_TRACE") // Done on purpose to let checked exception through
    public static Program parse(
            String src,
            CompilationUnitTree cut,
            WorkDirFileLoader workDirFileLoader,
            CoordinatorDfs dfs
    ) throws IOException, ScriptException {
        try {
            return cut.accept(INSTANCE, new Ctx(
                    cut.getSourceName(),
                    src,
                    workDirFileLoader,
                    dfs,
                    Parser.create("--language=es6"), // TODO:
                    new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe"), // TODO:
                    cut.getLineMap(),
                    State.COMPILATION_UNIT,
                    new ArrayList<>(),
                    new LinkedHashMap<>(),
                    new LinkedHashMap<>(),
                    new ArrayList<>()));
        } catch (WrappedIOException ex) {
            throw ex.getCause();
        } catch (WrappedScriptException ex) {
            throw ex.getCause();
        }
    }

    private static Program parseInvokedProgram(Ctx outerCtx, String src, CompilationUnitTree cut) {
        return cut.accept(INSTANCE, new Ctx(
                cut.getSourceName(),
                src,
                outerCtx.workDirFileLoader,
                outerCtx.dfs,
                outerCtx.parser,
                outerCtx.scriptEngine,
                cut.getLineMap(),
                State.COMPILATION_UNIT,
                new ArrayList<>(),
                new LinkedHashMap<>(),
                outerCtx.engineVars,
                new ArrayList<>()));
    }

    private static Program parseInnerExpression(Ctx outerCtx, Tree tree) {
        return tree.accept(INSTANCE, new Ctx(
                outerCtx.sourceName,
                outerCtx.sourceCode,
                outerCtx.workDirFileLoader,
                outerCtx.dfs,
                outerCtx.parser,
                outerCtx.scriptEngine,
                outerCtx.lineMap,
                State.BLOCK_OR_FN_CALL,
                new ArrayList<>(),
                outerCtx.constants,
                outerCtx.engineVars,
                new ArrayList<>()));
    }

    private final IterateTreeVisitor iterateTreeVisitor = new IterateTreeVisitor();

    private ProgramNashornTreeVisitor() {
    }

    protected record Ctx(String sourceName,
                         String sourceCode,
                         WorkDirFileLoader workDirFileLoader,
                         CoordinatorDfs dfs,
                         Parser parser,
                         ScriptEngine scriptEngine,
                         LineMap lineMap,
                         State state,
                         List<Src> candidateSrcs,
                         Map<String, Object> constants,
                         Map<String, Object> engineVars,
                         List<Op> ops) {
        public Ctx transitionState() {
            return withState(state.next());
        }

        public Ctx withState(State state) {
            return new Ctx(sourceName, sourceCode, workDirFileLoader, dfs, parser, scriptEngine, lineMap, state,
                    candidateSrcs, constants, engineVars, ops);
        }

        public void addCandidateSrc(Src src) {
            if(candidateSrcs.isEmpty())
                candidateSrcs.add(src);
            else
                candidateSrcs.addFirst(src);
        }
    }

    private enum State {
        COMPILATION_UNIT, BLOCK_OR_FN_CALL, CONSTANTS, EXPRESSION, ENGINE_FN_CALLS, SRC_FN_CALL, END;

        public State next() {
            return switch (this) {
                case COMPILATION_UNIT, BLOCK_OR_FN_CALL -> CONSTANTS;
                case CONSTANTS -> EXPRESSION;
                case EXPRESSION -> ENGINE_FN_CALLS;
                case ENGINE_FN_CALLS -> SRC_FN_CALL;
                case SRC_FN_CALL, END -> END;
            };
        }
    }

    @Override
    protected Program throwIllegalState(Tree node, Ctx ctx) {
        return throwIllegalState(
                "Unexpected tree node " + node.getKind() + " (state: " + ctx.state + ")",
                node, ctx);
    }

    private Program throwIllegalState(String msg, Tree node, Ctx ctx) {
        return throwIllegalState(msg, node, ctx, Collections.emptyList());
    }

    private Program throwIllegalState(String msg, Tree node, Ctx ctx, List<Throwable> suppressed) {
        throw createIllegalStateExc(msg, node, ctx, suppressed, false);
    }

    private IllegalStateException createIllegalStateExc(String msg, Tree node, Ctx ctx, List<Throwable> suppressed, boolean fast) {
        msg = msg +
                " at " + ctx.sourceName +
                ":" + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                ":" + ctx.lineMap().getColumnNumber(node.getStartPosition());
        var ex = fast ? new FastIllegalStateException(msg) : new IllegalStateException(msg);
        suppressed.forEach(ex::addSuppressed);
        throw ex;
    }

    @Override
    public Program visitCompilationUnit(CompilationUnitTree cut, Ctx ctx) {
        if(ctx.state != State.COMPILATION_UNIT)
            return throwIllegalState(cut, ctx);

        return visitCompilationUnitOrBlock(cut.getSourceElements(), ctx);
    }

    @Override
    public Program visitBlock(BlockTree node, Ctx ctx) {
        if(ctx.state != State.BLOCK_OR_FN_CALL)
            return throwIllegalState(node, ctx);

        return visitCompilationUnitOrBlock(node.getStatements(), ctx);
    }

    public Program visitCompilationUnitOrBlock(Iterable<? extends Tree> statements, Ctx ctx) {
        var newCtx = ctx.transitionState();
        for (var iterator = statements.iterator(); iterator.hasNext(); ) {
            Tree sourceElement = iterator.next();
            
            boolean isLast = !iterator.hasNext();
            if(isLast)
                return sourceElement.accept(this, newCtx.transitionState());
            
            sourceElement.accept(this, newCtx);
        }
        
        throw new UnsupportedOperationException("Compilation unit is empty, missing engine call");
    }

    @Override
    @SuppressWarnings({ "NullAway", "DataFlowIssue" })
    public Program visitVariable(VariableTree node, Ctx ctx) {
        if(ctx.state != State.CONSTANTS)
            return throwIllegalState(node, ctx);

        if(!node.isConst())
            return throwIllegalState("Non constant top-level variables are not supported", node, ctx);
        if(!(node.getBinding() instanceof IdentifierTree identifier))
            return throwIllegalState("Constant top-level variables declared without identifier are not supported", node, ctx);
        
        Object value = parseLiteral(
                ctx,
                node.getInitializer(),
                "top-level constant " + identifier.getName(),
                Object.class);
        boolean wasAlreadyPresent = ctx.constants.putIfAbsent(identifier.getName(), value) != null;
        if(wasAlreadyPresent)
            return throwIllegalState("Top-level constant " + identifier.getName() + " was already declared", node, ctx);
            
        return null;
    }

    @Override
    public Program visitExpressionStatement(ExpressionStatementTree node, Ctx ctx) {
        if(ctx.state != State.EXPRESSION)
            return throwIllegalState(node, ctx);

        boolean isSetupAndExec = node.getExpression() instanceof FunctionCallTree execFct &&
                execFct.getFunctionSelect() instanceof MemberSelectTree execMst &&
                execMst.getIdentifier().equals("exec");
        return isSetupAndExec 
                ? parseSetupAndExec(ctx, node)
                : node.getExpression().accept(this, ctx.transitionState());
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS") // Done on purpose, exception will be unwrapped later
    private Program parseSetupAndExec(Ctx ctx, ExpressionStatementTree node) {

        if(!(node.getExpression() instanceof FunctionCallTree execFct) ||
                !(execFct.getFunctionSelect() instanceof MemberSelectTree execMst) ||
                !execMst.getIdentifier().equals("exec") ||
                execFct.getArguments().size() != 1 ||
                !(execFct.getArguments().getFirst() instanceof FunctionExpressionTree execInnerFnExpr))
            return throwIllegalState("Missing proper exec call in engine.setup(...).exec(...)", node, ctx);

        if(!(execMst.getExpression() instanceof FunctionCallTree setupFct) ||
                !(setupFct.getFunctionSelect() instanceof MemberSelectTree setupMst) ||
                !setupMst.getIdentifier().equals("setup"))
            return throwIllegalState("Missing proper setup call in engine.setup(...).exec(...)", node, ctx);

        if(!(setupMst.getExpression() instanceof IdentifierTree engineIdentifier) || !engineIdentifier.getName().equals("engine"))
            return throwIllegalState("Missing engine in engine.setup(...).exec(...)", node, ctx);

        ConfiguredEngine configuredEngine;
        try {
            configuredEngine = ConfiguredEngine.runFor(ctx.scriptEngine, extractBodyOfFnParameter(ctx, setupFct, setupMst));
        } catch (ScriptException e) {
            throw new WrappedScriptException(e);
        }

        ctx.engineVars.putAll(configuredEngine.getVars());
        return parseInnerExpression(ctx, execInnerFnExpr.getBody());
    }

    @Override
    public Program visitFunctionCall(FunctionCallTree node, Ctx ctx) {
        if(ctx.state == State.BLOCK_OR_FN_CALL)
            ctx = ctx.withState(State.ENGINE_FN_CALLS);

        if(ctx.state != State.ENGINE_FN_CALLS)
            return throwIllegalState(node, ctx);

        return visitEngineFunctionCall(node, ctx);
    }

    public Program visitEngineFunctionCall(FunctionCallTree node, Ctx ctx) {
        if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
            return throwIllegalState(node.getFunctionSelect(), ctx);

        var maybeKind = ExtendedOpKind.VALUES.stream()
                .filter(k -> k.getName().equals(mst.getIdentifier()))
                .findFirst();

        if(maybeKind.isEmpty())
            return visitSrcFnCall(node, mst, ctx.transitionState());

        boolean expectsTerminal = ctx.ops.isEmpty();
        parseEngineFnOp(node, mst, ctx, ctx.ops, maybeKind.get(), expectsTerminal);
        return mst.getExpression().accept(this, ctx);
    }

    private void parseEngineFnOp(
            FunctionCallTree node,
            MemberSelectTree mst,
            Ctx ctx,
            List<Op> ops,
            ExtendedOpKind kind,
            boolean expectsTerminal
    ) {
        switch (kind) {
            case ExtendedOpKind.Normal n -> parseNormalEngineFnOp(node, mst, ctx, ops, n.kind(), expectsTerminal);
            case ExtendedOpKind.Ext.RUN -> parseRunEngineFnOp(node, ctx, ops);
            case ExtendedOpKind.Ext.ITERATE -> parseIterateEngineFnOp(node, mst, ctx, ops);
        }
    }

    private void parseNormalEngineFnOp(
            FunctionCallTree node,
            MemberSelectTree mst,
            Ctx ctx,
            List<Op> ops,
            OpKind kind,
            boolean expectsTerminal
    ) {
        if(!expectsTerminal && kind.isTerminal()) {
            throwIllegalState(
                    "Unrecognized operator " + mst.getIdentifier() + " (expected non-terminal, got terminal)",
                    node, ctx);
            return;
        }

        if(node.getArguments().size() != 1 || !(node.getArguments().getFirst() instanceof FunctionExpressionTree)) {
            throwIllegalState(mst.getExpression(), ctx);
            return;
        }

        var constantsDecls = ctx.constants.entrySet().stream()
                .map(e -> {
                    var val = e.getValue() instanceof String s ? '"' + s + '"' : e.getValue();
                    return STR."const \{e.getKey()} = \{val};";
                })
                .collect(Collectors.joining("\n    ", "    ", ""));

        var engineVarsDecls = ctx.engineVars.entrySet().stream()
                .map(e -> {
                    var val = e.getValue() instanceof String s ? '"' + s + '"' : e.getValue();
                    return STR."if(name === '\{e.getKey()}') return \{val};";
                })
                .collect(Collectors.joining("\n            "));
        var engineVarsDecl = STR."""
            const engineVars = (() => {
                const raw = (name) => {
                    \{engineVarsDecls}
                    return undefined;
                };
                return {
                    number: (name) => Number(raw(name)),
                    string: (name) => String(raw(name)),
                    boolean: (name) => !!raw(name),
                };
            })();\
        """;

        @SuppressWarnings("TrailingWhitespacesInTextBlock")
        var bakedVarsSrc = STR."""
                (() => {
                \{constantsDecls}
                \{engineVarsDecl}
                    return \{extractBodyOfFnParameter(ctx, node, mst)};
                })()\
                """;

        var op = new Op(kind, bakedVarsSrc);
        if(ops.isEmpty())
            ops.add(op);
        else
            ops.addFirst(op);
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS") // Done on purpose, exception will be unwrapped later
    private void parseRunEngineFnOp(
            FunctionCallTree node,
            Ctx ctx,
            List<Op> ops
    ) {
        // Parse first parameter as a string literal
        String otherScriptPath = parseLiteral(ctx, node.getArguments().getFirst(), "arg 0 of run", String.class);

        String otherScriptSrc;
        try (InputStream is = ctx.workDirFileLoader.loadResourceAsStream(otherScriptPath)) {
            otherScriptSrc = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new WrappedIOException(new IOException("Failed to parse invoked program " + otherScriptPath, e));
        }

        CompilationUnitTree otherScriptCut = ctx.parser.parse(
                otherScriptPath, otherScriptSrc, info -> LOGGER.error(info.getMessage()));
        if (otherScriptCut == null)
            throw new IllegalStateException("Failed to compile invoked program " + otherScriptPath);

        Program otherScriptProgram = parseInvokedProgram(ctx, otherScriptSrc, otherScriptCut);
        ops.addAll(0, otherScriptProgram.ops());
        ctx.addCandidateSrc(otherScriptProgram.src());
    }

    private void parseIterateEngineFnOp(
            FunctionCallTree node,
            MemberSelectTree mst,
            Ctx ctx,
            List<Op> ops
    ) {
        if(node.getArguments().size() != 2) {
            throwIllegalState(mst.getExpression(), ctx);
            return;
        }

        // Parse first parameter as an integer literal
        int iterations = parseLiteral(ctx, node.getArguments().getFirst(), "arg 0 of iterate", Integer.class);

        // Parse second parameter as a function with at least 1 parameter
        if(!(node.getArguments().get(1) instanceof FunctionExpressionTree fet) ||
                fet.getParameters().isEmpty() ||
                !(fet.getParameters().getFirst() instanceof IdentifierTree iterateBodyParam)) {
            throwIllegalState(String.format(Locale.ROOT,
                    "Expected arg 1 of iterate to be a function with at least 1 (identifier) parameter, got %s (state: %s)",
                    node.getArguments().get(1) instanceof FunctionExpressionTree fet
                            ? "parameters " + fet.getParameters()
                            : node.getArguments().get(1),
                    ctx.state
            ), node, ctx);
            return;
        }

        final List<Op> iterationOps = fet.getBody().accept(
                iterateTreeVisitor,
                new IterateCtx(ctx, iterateBodyParam, new ArrayList<>()));

        for(int i = 0; i < iterations; i++) {
            ops.addAll(0, iterationOps);
        }
    }

    private record IterateCtx(Ctx outerCtx, IdentifierTree iterateBodyParam, List<Op> iterationOps) {
    }

    private final class IterateTreeVisitor extends ThrowingNashornTreeVisitor<List<Op>, IterateCtx> {

        @Override
        protected List<Op> throwIllegalState(Tree node, IterateCtx iterateCtx) {
            ProgramNashornTreeVisitor.this.throwIllegalState(node, iterateCtx.outerCtx);
            throw new AssertionError("Should not get here");
        }

        @SuppressWarnings("UnusedReturnValue")
        private List<Op> throwIllegalState(String msg, Tree node, IterateCtx iterateCtx) {
            ProgramNashornTreeVisitor.this.throwIllegalState(msg, node, iterateCtx.outerCtx);
            throw new AssertionError("Should not get here");
        }

        @Override
        public List<Op> visitFunctionCall(FunctionCallTree node, IterateCtx ctx) {
            if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
                return throwIllegalState(node.getFunctionSelect(), ctx);

            var kind = ExtendedOpKind.VALUES.stream()
                    .filter(k -> k.getName().equals(mst.getIdentifier()))
                    .findFirst()
                    .orElseThrow(() -> {
                        throwIllegalState("Unrecognized operator " + mst.getIdentifier(), node, ctx);
                        return new AssertionError("Not reachable");
                    });
            parseEngineFnOp(node, mst, ctx.outerCtx, ctx.iterationOps, kind, false);
            return mst.getExpression().accept(this, ctx);
        }

        @Override
        public List<Op> visitIdentifier(IdentifierTree node, IterateCtx ctx) {
            if(!node.getName().equals(ctx.iterateBodyParam.getName()))
                return throwIllegalState(node, ctx);

            return ctx.iterationOps;
        }
    }

    private Program visitSrcFnCall(FunctionCallTree node, MemberSelectTree mst, Ctx ctx) {
        var kind = CoordinatorSrc.Kind.VALUES.stream()
                .filter(k -> k.getMethodIdentifier().equals(mst.getIdentifier()))
                .findFirst()
                .orElseThrow(() -> {
                    throwIllegalState("Unrecognized src operator " + mst.getIdentifier(), node, ctx);
                    return new AssertionError("Not reachable");
                });

        final ObjectLiteralTree olt = kind == CoordinatorSrc.Kind.REQUIRE ?
                null :
                node.getArguments().size() == 1 && node.getArguments().getFirst() instanceof ObjectLiteralTree olt0 ?
                        olt0 :
                        null;
        if(switch (kind) {
            case REQUIRE -> node.getArguments().size() > 1 ||
                    (node.getArguments().size() == 1 && !(node.getArguments().getFirst() instanceof FunctionExpressionTree));
            default -> olt == null && kind.getMinArgs() != 0;
        }) {
            return throwIllegalState(String.format(Locale.ROOT,
                            kind == CoordinatorSrc.Kind.REQUIRE
                                    ? "Expected a fn literal arg for src %s, got %s"
                                    : "Expected only an object literal arg for src %s, got %s",
                            kind, node.getArguments().size() == 1
                                    ? node.getArguments().getFirst()
                                    : node.getArguments()),
                    node, ctx);
        }

        final Map<String, Object> parsedArgs = olt == null ? Collections.emptyMap() : olt.getProperties().stream()
                .filter(pt -> pt.getKey() instanceof IdentifierTree)
                .collect(Collectors.toMap(
                        pt -> ((IdentifierTree) pt.getKey()).getName(),
                        pt -> {
                            final String argName = ((IdentifierTree) pt.getKey()).getName();
                            final var expectedArg = kind.getArgs().get(argName);
                            if(expectedArg == null)
                                return new FastIllegalStateException("Unknown property " + argName  + " for src " + kind);

                            return tryParseLiteral(
                                    ctx,
                                    pt.getValue(),
                                    "property " + argName  + " of src " + kind,
                                    expectedArg.type());
                        }));

        final List<Throwable> exs = olt == null ? new ArrayList<>() : olt.getProperties().stream()
                .filter(pt -> !(pt.getKey() instanceof IdentifierTree))
                .map(pt -> new FastIllegalStateException("Unexpected property " + pt))
                .collect(Collectors.toList());
        exs.addAll(parsedArgs.values().stream()
                .filter(Throwable.class::isInstance)
                .map(Throwable.class::cast)
                .toList());
        exs.addAll(kind.getArgs().values().stream()
                .filter(a -> a.required() && parsedArgs.get(a.name()) == null)
                .map(a -> new FastIllegalStateException("Missing required argument " + a.name()))
                .toList());

        if(!exs.isEmpty())
            return throwIllegalState("Invalid src operator usage for " + kind, node, ctx, exs);

        Function<String, Object> requireArg = name -> Objects.requireNonNull(parsedArgs.get(name));
        CoordinatorSrc src = switch (kind) {
            case LINES -> new LinesSrc(
                    ctx.workDirFileLoader(),
                    (String) requireArg.apply("file"),
                    (int) requireArg.apply("partitions"),
                    (String) parsedArgs.get("srcDfsFile"),
                    (String) parsedArgs.get("dstDfsFile")
            );
            case CSV -> new CsvSrc(
                    ctx.workDirFileLoader(),
                    (String) requireArg.apply("file"),
                    (int) requireArg.apply("partitions"),
                    (String) parsedArgs.get("delimiter"),
                    (String) parsedArgs.get("srcDfsFile"),
                    (String) parsedArgs.get("dstDfsFile")
            );
            case DFS -> new DfsSrc(
                    ctx.dfs(),
                    (String) requireArg.apply("file"),
                    (String) parsedArgs.get("dstDfsFile")
            );
            case REQUIRE -> new RequireSrc();
        };

        ctx.addCandidateSrc(src);
        return mst.getExpression().accept(this, ctx.transitionState());
    }

    @Override
    public Program visitIdentifier(IdentifierTree node, Ctx ctx) {
        if(ctx.state != State.END)
            return throwIllegalState(node, ctx);

        if(!node.getName().equals("engine"))
            return throwIllegalState(node, ctx);

        if(ctx.candidateSrcs.isEmpty())
            throw new UnsupportedOperationException("Missing source");

        final var pickedSrc = ctx.candidateSrcs.stream()
                .filter(s -> !(s instanceof RequireSrc))
                .findFirst()
                .orElse(ctx.candidateSrcs.getFirst());
        return new Program(pickedSrc, ctx.ops());
    }

    @SuppressWarnings("unchecked")
    private <T> T parseLiteral(Ctx ctx, ExpressionTree node, String literalName, Class<T> expectedType) {
        return (T) doParseLiteral(ctx, node, literalName, expectedType, true);
    }

    private Object tryParseLiteral(Ctx ctx, ExpressionTree node, String literalName, Class<?> expectedType) {
        return doParseLiteral(ctx, node, literalName, expectedType, false);
    }

    @SuppressFBWarnings({
            "ITC_INHERITANCE_TYPE_CHECKING", // Literally what the method is supposed to be doing
            "URV_UNRELATED_RETURN_VALUES", // Which is why you shouldn't use this directly, a bit of a mess
    })
    private Object doParseLiteral(
            Ctx ctx,
            ExpressionTree node,
            String literalName,
            Class<?> expectedType,
            boolean throwErrors
    ) {

        Object wrongValue = node;
        if (node instanceof LiteralTree lt) {
            if(expectedType.isInstance(lt.getValue()))
                return lt.getValue();

            wrongValue = lt.getValue();
        }

        Object candidate = null;
        if (node instanceof IdentifierTree id) {
            if((candidate = ctx.constants.get(id.getName())) != null && expectedType.isInstance(candidate))
                return candidate;

            wrongValue = candidate != null ? candidate : "unknown identifier " + id.getName();
        }

        if (node instanceof FunctionCallTree fct &&
                fct.getFunctionSelect() instanceof MemberSelectTree mst &&
                mst.getExpression() instanceof IdentifierTree id &&
                id.getName().equals("engineVars")) {

            boolean isValidMethod = (mst.getIdentifier().equals("string") ||
                    mst.getIdentifier().equals("number") ||
                    mst.getIdentifier().equals("boolean"));
            @SuppressWarnings("PMD.UnusedAssignment") // It's a compile error to not initialize it here
            Object firstParam = null;

            if(isValidMethod &&
                    !fct.getArguments().isEmpty() &&
                    fct.getArguments().getFirst() instanceof LiteralTree firstArgLt &&
                    (firstParam = firstArgLt.getValue()) instanceof String engineVarName &&
                    (candidate = ctx.engineVars.get(engineVarName)) != null &&
                    expectedType.isInstance(candidate))
                return candidate;

            wrongValue = !isValidMethod ?
                    "unknown engineVars function " + mst.getIdentifier() :
                    firstParam == null ?
                            "engineVars#" + mst.getIdentifier() + "(StringLiteral) missing param" :
                            !(firstParam instanceof String) ?
                                    "engineVars#" + mst.getIdentifier() + "(string) unexpected param " + firstParam :
                                    candidate;

        }

        var msg = String.format(Locale.ROOT,
                "Expected %s to be a literal %s, got %s (state: %s)",
                literalName, expectedType, wrongValue, ctx.state);

        if(throwErrors) {
            throwIllegalState(msg, node, ctx);
            throw new AssertionError("Not reachable");
        }

        return createIllegalStateExc(msg, node, ctx, Collections.emptyList(), true);
    }

    @SuppressFBWarnings(
            value = "STT_STRING_PARSING_A_FIELD",
            justification = "This method is parsing source code, so it's doing exactly what's intended")
    private String extractBodyOfFnParameter(Ctx ctx, FunctionCallTree node, MemberSelectTree mst) {
        return ctx.sourceCode.substring(
                (int) mst.getEndPosition(),
                (int) node.getEndPosition()
        ).replace("\r", "");
    }
}
