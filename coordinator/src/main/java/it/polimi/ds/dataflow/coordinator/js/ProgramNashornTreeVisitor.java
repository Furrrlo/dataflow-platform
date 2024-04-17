package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.src.CoordinatorSrc;
import it.polimi.ds.dataflow.coordinator.src.CsvSrc;
import it.polimi.ds.dataflow.coordinator.src.DfsSrc;
import it.polimi.ds.dataflow.coordinator.src.LinesSrc;
import it.polimi.ds.dataflow.js.Op;
import it.polimi.ds.dataflow.js.OpKind;
import it.polimi.ds.dataflow.js.Program;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;
import org.openjdk.nashorn.api.tree.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

@SuppressWarnings({
        "ClassEscapesDefinedScope" // There's no way to get an instance of this class, it can't escape
})
public final class ProgramNashornTreeVisitor extends ThrowingNashornTreeVisitor<Program, ProgramNashornTreeVisitor.Ctx> {

    private static final ProgramNashornTreeVisitor INSTANCE = new ProgramNashornTreeVisitor();

    public static Program parse(String src,
                                CompilationUnitTree cut,
                                WorkDirFileLoader workDirFileLoader,
                                CoordinatorDfs dfs) {
        return cut.accept(INSTANCE, new Ctx(
                src,
                workDirFileLoader,
                dfs,
                cut.getLineMap(),
                State.COMPILATION_UNIT,
                null, new ArrayList<>()));
    }

    private final IterateTreeVisitor iterateTreeVisitor = new IterateTreeVisitor();

    private ProgramNashornTreeVisitor() {
    }

    protected record Ctx(String sourceCode,
                         WorkDirFileLoader workDirFileLoader,
                         CoordinatorDfs dfs,
                         LineMap lineMap,
                         State state,
                         @Nullable CoordinatorSrc src,
                         List<Op> ops) {
        public Ctx transitionState() {
            return new Ctx(sourceCode, workDirFileLoader, dfs, lineMap, state.next(), src, ops);
        }

        public Ctx withSrc(CoordinatorSrc src) {
            return new Ctx(sourceCode, workDirFileLoader, dfs, lineMap, state, src, ops);
        }
    }

    private enum State {
        COMPILATION_UNIT, EXPRESSION, ENGINE_FN_CALLS, SRC_FN_CALL, END;

        public State next() {
            return switch (this) {
                case COMPILATION_UNIT -> EXPRESSION;
                case EXPRESSION -> ENGINE_FN_CALLS;
                case ENGINE_FN_CALLS -> SRC_FN_CALL;
                case SRC_FN_CALL, END -> END;
            };
        }
    }

    @Override
    @SuppressWarnings("TrailingWhitespacesInTextBlock")
    protected Program throwIllegalState(Tree node, Ctx ctx) {
        throw new IllegalStateException(STR."""
            Unexpected tree node \{node.getKind()} (state: \{ctx.state}) \
            at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
            \{ctx.lineMap().getColumnNumber(node.getStartPosition())}
            """);
    }

    @Override
    public Program visitCompilationUnit(CompilationUnitTree cut, Ctx ctx) {
        if(ctx.state != State.COMPILATION_UNIT)
            return throwIllegalState(cut, ctx);
        if(cut.getSourceElements().size() != 1)
            return throwIllegalState(cut, ctx);

        return cut.getSourceElements().getFirst().accept(this, ctx.transitionState());
    }

    @Override
    public Program visitExpressionStatement(ExpressionStatementTree node, Ctx ctx) {
        if(ctx.state != State.EXPRESSION)
            return throwIllegalState(node, ctx);

        return node.getExpression().accept(this, ctx.transitionState());
    }

    @Override
    public Program visitFunctionCall(FunctionCallTree node, Ctx ctx) {
        if(ctx.state != State.ENGINE_FN_CALLS)
            return throwIllegalState(node, ctx);

        return visitEngineFunctionCall(node, ctx);
    }

    public Program visitEngineFunctionCall(FunctionCallTree node, Ctx ctx) {
        if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
            return throwIllegalState(node.getFunctionSelect(), ctx);

        var maybeKind = OpKind.VALUES.stream()
                .filter(k -> k.getName().equals(mst.getIdentifier()))
                .findFirst();

        if(maybeKind.isEmpty())
            return visitSrcFnCall(node, mst, ctx.transitionState());

        boolean expectsTerminal = ctx.ops.isEmpty();
        parseEngineFnOp(node, mst, ctx, ctx.ops, maybeKind.get(), expectsTerminal);
        return mst.getExpression().accept(this, ctx);
    }

    @SuppressWarnings("TrailingWhitespacesInTextBlock")
    @SuppressFBWarnings(
            value = "STT_STRING_PARSING_A_FIELD",
            justification = "This method is parsing source code, so it's doing exactly what's intended")
    private void parseEngineFnOp(
            FunctionCallTree node,
            MemberSelectTree mst,
            Ctx ctx,
            List<Op> ops,
            OpKind kind,
            boolean expectsTerminal
    ) {
        if(!expectsTerminal && kind.isTerminal())
            throw new IllegalStateException(STR."""
                Unrecognized operator \{mst.getIdentifier()} \
                (expected non-terminal, got terminal) \
                at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
                \{ctx.lineMap().getColumnNumber(node.getStartPosition())}
                """);

        // Special case: iterate
        if(kind == OpKind.ITERATE) {
            if(node.getArguments().size() != 2) {
                throwIllegalState(mst.getExpression(), ctx);
                return;
            }

            // Parse first parameter as an integer literal
            if(!(node.getArguments().getFirst() instanceof LiteralTree lt) ||
                    !(lt.getValue() instanceof Integer iterations))
                throw new IllegalStateException(STR."""
                    Expected arg 0 to be a literal integer, \
                    got \{node.getArguments().getFirst() instanceof LiteralTree lt
                        ? lt.getValue()
                        : node.getArguments().getFirst()} \
                    (state: \{ctx.state}, kind: \{kind}) \
                    at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
                    \{ctx.lineMap().getColumnNumber(node.getStartPosition())}
                    """);

            // Parse second parameter as a function with at least 1 parameter
            if(!(node.getArguments().get(1) instanceof FunctionExpressionTree fet) ||
                    fet.getParameters().isEmpty() ||
                    !(fet.getParameters().getFirst() instanceof IdentifierTree iterateBodyParam))
                throw new IllegalStateException(STR."""
                    Expected arg 1 to be a function with at least 1 (identifier) parameter, \
                    got \{node.getArguments().get(1) instanceof FunctionExpressionTree fet
                        ? "parameters " + fet.getParameters()
                        : node.getArguments().get(1) } \
                    (state: \{ctx.state}, kind: \{kind}) \
                    at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
                    \{ctx.lineMap().getColumnNumber(node.getStartPosition())}
                    """);

            final List<Op> iterationOps = fet.getBody().accept(
                    iterateTreeVisitor,
                    new IterateCtx(ctx, iterateBodyParam, new ArrayList<>()));

            for(int i = 0; i < iterations; i++)
                ctx.ops.addAll(0, iterationOps);
            return;
        }

        if(node.getArguments().size() != 1 || !(node.getArguments().getFirst() instanceof FunctionExpressionTree)) {
            throwIllegalState(mst.getExpression(), ctx);
            return;
        }

        var op = new Op(kind, ctx.sourceCode.substring(
                (int) mst.getEndPosition(),
                (int) node.getEndPosition()
        ).replace("\r", ""));
        if(ops.isEmpty())
            ops.add(op);
        else
            ops.addFirst(op);
    }

    private record IterateCtx(Ctx outerCtx, IdentifierTree iterateBodyParam, List<Op> iterationOps) {
    }

    private class IterateTreeVisitor extends ThrowingNashornTreeVisitor<List<Op>, IterateCtx> {

        @Override
        protected List<Op> throwIllegalState(Tree node, IterateCtx iterateCtx) {
            ProgramNashornTreeVisitor.this.throwIllegalState(node, iterateCtx.outerCtx);
            throw new AssertionError("Should not get here");
        }

        @Override
        @SuppressWarnings("TrailingWhitespacesInTextBlock")
        public List<Op> visitFunctionCall(FunctionCallTree node, IterateCtx ctx) {
            if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
                return throwIllegalState(node.getFunctionSelect(), ctx);

            var kind = OpKind.VALUES.stream()
                    .filter(k -> k.getName().equals(mst.getIdentifier()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(STR."""
                                    Unrecognized operator \{mst.getIdentifier()} \
                                    at line \{ctx.outerCtx.lineMap().getLineNumber(node.getStartPosition())}:\
                                    \{ctx.outerCtx.lineMap().getColumnNumber(node.getStartPosition())}
                                    """));
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

    @SuppressWarnings("TrailingWhitespacesInTextBlock")
    private Program visitSrcFnCall(FunctionCallTree node, MemberSelectTree mst, Ctx ctx) {
        var kind = CoordinatorSrc.Kind.VALUES.stream()
                .filter(k -> k.getMethodIdentifier().equals(mst.getIdentifier()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(STR."""
                    Unrecognized src operator \{mst.getIdentifier()} \
                    at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
                    \{ctx.lineMap().getColumnNumber(node.getStartPosition())}\
                    """));

        if(node.getArguments().size() < kind.getMinArgs() || node.getArguments().size() > kind.getMaxArgs())
            return throwIllegalState(mst.getExpression(), ctx);

        final List<Object> parsedArgs = IntStream.range(0, node.getArguments().size()).mapToObj(argIdx -> {
            final Class<?> expectedType = kind.getArgs().get(argIdx);

            if (!(node.getArguments().get(argIdx) instanceof LiteralTree lt) || !expectedType.isInstance(lt.getValue()))
                throw new IllegalStateException(STR."""
                    Expected arg \{argIdx} \
                    to be a literal \{expectedType}, \
                    got \{node.getArguments().get(argIdx) instanceof LiteralTree lt
                        ? lt.getValue()
                        : node.getArguments().get(argIdx)} \
                    (state: \{ctx.state}, kind: \{kind}) \
                    at line \{ctx.lineMap().getLineNumber(node.getStartPosition())}:\
                    \{ctx.lineMap().getColumnNumber(node.getStartPosition())}
                    """);

            return lt.getValue();
        }).toList();

        CoordinatorSrc src = switch (kind) {
            case LINES -> new LinesSrc(ctx.workDirFileLoader(), (String) parsedArgs.getFirst(), (int) parsedArgs.get(1));
            case CSV -> switch (parsedArgs.size()) {
                case 2 ->  new CsvSrc(ctx.workDirFileLoader(), (String) parsedArgs.getFirst(), (int) parsedArgs.get(1));
                case 3 ->  new CsvSrc(ctx.workDirFileLoader(),
                        (String) parsedArgs.getFirst(), (int) parsedArgs.get(1), (String) parsedArgs.get(2));
                default -> throw new AssertionError(STR."Unexpected parsing error, unrecognized params \{parsedArgs}");
            };
            case DFS -> new DfsSrc(ctx.dfs(), (String) parsedArgs.getFirst());
        };

        return mst.getExpression().accept(this, ctx.withSrc(src).transitionState());
    }

    @Override
    public Program visitIdentifier(IdentifierTree node, Ctx ctx) {
        if(ctx.state != State.END)
            return throwIllegalState(node, ctx);

        if(!node.getName().equals("engine"))
            return throwIllegalState(node, ctx);

        return new Program(Objects.requireNonNull(ctx.src(), "Missing source"), ctx.ops());
    }
}
