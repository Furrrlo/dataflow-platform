package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.coordinator.src.CoordinatorSrc;
import it.polimi.ds.dataflow.coordinator.src.CsvSrc;
import it.polimi.ds.dataflow.coordinator.src.DfsSrc;
import it.polimi.ds.dataflow.coordinator.src.LinesSrc;
import it.polimi.ds.map_reduce.js.Op;
import it.polimi.ds.map_reduce.js.OpKind;
import it.polimi.ds.map_reduce.js.Program;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;
import org.openjdk.nashorn.api.tree.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@SuppressWarnings({
        "ClassEscapesDefinedScope" // There's no way to get an instance of this class, it can't escape
})
public final class ProgramNashornTreeVisitor extends ThrowingNashornTreeVisitor<Program, ProgramNashornTreeVisitor.Ctx> {

    private static final ProgramNashornTreeVisitor INSTANCE = new ProgramNashornTreeVisitor();

    public static Program parse(String src,
                                CompilationUnitTree cut,
                                LocalSrcFileLoader localFileLoader,
                                @Nullable CoordinatorDfs dfs) {
        return cut.accept(INSTANCE, new Ctx(
                src,
                localFileLoader,
                dfs,
                cut.getLineMap(),
                State.COMPILATION_UNIT,
                null, 0, new ArrayList<>()));
    }

    private ProgramNashornTreeVisitor() {
    }

    protected record Ctx(String sourceCode,
                         LocalSrcFileLoader localFileLoader,
                         @Nullable CoordinatorDfs dfs,
                         LineMap lineMap,
                         State state,
                         @Nullable CoordinatorSrc src,
                         int partitions,
                         List<Op> ops) {
        public Ctx transitionState() {
            return new Ctx(sourceCode, localFileLoader, dfs, lineMap, state.next(), src, partitions, ops);
        }

        public Ctx withPartitions(int partitions) {
            return new Ctx(sourceCode, localFileLoader, dfs, lineMap, state, src, partitions, ops);
        }

        public Ctx withSrc(CoordinatorSrc src) {
            return new Ctx(sourceCode, localFileLoader, dfs, lineMap, state, src, partitions, ops);
        }
    }

    private enum State {
        COMPILATION_UNIT, EXPRESSION, ENGINE_FN_CALLS, OPTION_FN_CALL, SRC_FN_CALL, END;

        public State next() {
            return switch (this) {
                case COMPILATION_UNIT -> EXPRESSION;
                case EXPRESSION -> ENGINE_FN_CALLS;
                case ENGINE_FN_CALLS -> OPTION_FN_CALL;
                case OPTION_FN_CALL -> SRC_FN_CALL;
                case SRC_FN_CALL, END -> END;
            };
        }
    }

    @Override
    protected Program throwIllegalState(Tree node, Ctx ctx) {
        throw new IllegalStateException("Unexpected tree node " + node.getKind() +
                " (state: " + ctx.state + ") " +
                " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));
    }

    @Override
    public Program visitCompilationUnit(CompilationUnitTree cut, Ctx ctx) {
        if(ctx.state != State.COMPILATION_UNIT)
            return throwIllegalState(cut, ctx);
        if(cut.getSourceElements().size() != 1)
            return throwIllegalState(cut, ctx);

        return cut.getSourceElements().get(0).accept(this, ctx.transitionState());
    }

    @Override
    public Program visitExpressionStatement(ExpressionStatementTree node, Ctx ctx) {
        if(ctx.state != State.EXPRESSION)
            return throwIllegalState(node, ctx);

        return node.getExpression().accept(this, ctx.transitionState());
    }

    @Override
    public Program visitFunctionCall(FunctionCallTree node, Ctx ctx) {
        return switch (ctx.state) {
            case ENGINE_FN_CALLS -> visitEngineFunctionCall(node, ctx);
            case SRC_FN_CALL -> visitSrcFnCall(node, ctx);
            default -> throwIllegalState(node, ctx);
        };
    }

    @SuppressFBWarnings(
            value = "STT_STRING_PARSING_A_FIELD",
            justification = "This method is parsing source code, so it's doing exactly what's intended")
    public Program visitEngineFunctionCall(FunctionCallTree node, Ctx ctx) {
        if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
            return throwIllegalState(node.getFunctionSelect(), ctx);

        var maybeKind = Arrays.stream(OpKind.values())
                .filter(k -> k.getName().equals(mst.getIdentifier()))
                .findFirst();

        if(maybeKind.isEmpty()) {
            if(mst.getIdentifier().equals("partitions"))
                return visitOptionsFnCall(node, mst, ctx.transitionState());

            throw new IllegalStateException("Unrecognized operator " + mst.getIdentifier() +
                    " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                    ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));
        }

        OpKind kind = maybeKind.get();
        boolean expectsTerminal = ctx.ops.isEmpty();

        if(!expectsTerminal && kind.isTerminal())
            throw new IllegalStateException("Unrecognized operator " + mst.getIdentifier() +
                    "(expected non-terminal, got terminal)" +
                    " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                    ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));

        if(node.getArguments().size() != 1 || !(node.getArguments().get(0) instanceof FunctionExpressionTree))
            return throwIllegalState(mst.getExpression(), ctx);

        var op = new Op(kind, ctx.sourceCode.substring(
                (int) mst.getEndPosition(),
                (int) node.getEndPosition()));
        if(ctx.ops.isEmpty())
            ctx.ops.add(op);
        else
            ctx.ops.add(0, op);

        return mst.getExpression().accept(this, ctx);
    }

    private Program visitOptionsFnCall(FunctionCallTree node, MemberSelectTree mst, Ctx ctx) {
        if(node.getArguments().size() != 1)
            return throwIllegalState(node, ctx);

        if(!(node.getArguments().get(0) instanceof LiteralTree lt) || !(lt.getValue() instanceof Number partitions))
            throw new IllegalStateException("Expected partitions to be a literal number, got " + node.getKind() +
                    " (state: " + ctx.state + ") " +
                    " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                    ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));

        return mst.getExpression().accept(
                this,
                ctx.withPartitions(partitions.intValue()).transitionState());
    }

    private Program visitSrcFnCall(FunctionCallTree node, Ctx ctx) {
        if(!(node.getFunctionSelect() instanceof MemberSelectTree mst))
            return throwIllegalState(node.getFunctionSelect(), ctx);

        var kind = Arrays.stream(CoordinatorSrc.Kind.values())
                .filter(k -> k.getMethodIdentifier().equals(mst.getIdentifier()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unrecognized src operator " + mst.getIdentifier() +
                        " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                        ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition())));

        int maxExpectedArgs = switch (kind) {
            // TODO: declare args on the enum
            case LINES, DFS -> 1;
            case CSV -> 2;
        };

        if(node.getArguments().isEmpty() || node.getArguments().size() > maxExpectedArgs)
            return throwIllegalState(mst.getExpression(), ctx);

        if(!(node.getArguments().get(0) instanceof LiteralTree lt) || !(lt.getValue() instanceof String fileName))
            throw new IllegalStateException("Expected fileName to be a literal string, got " + node.getKind() +
                    " (state: " + ctx.state + ") " +
                    " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                    ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));

        CoordinatorSrc src = switch (kind) {
            case LINES -> new LinesSrc(ctx.localFileLoader(), fileName);
            case CSV -> {
                if(node.getArguments().size() == 1)
                    yield new CsvSrc(ctx.localFileLoader(), fileName);

                if(!(node.getArguments().get(1) instanceof LiteralTree dlt) ||
                        !(dlt.getValue() instanceof String delimiter))
                    throw new IllegalStateException("Expected csv delimiter to be a literal string, got " + node.getKind() +
                            " (state: " + ctx.state + ") " +
                            " at line " + ctx.lineMap().getLineNumber(node.getStartPosition()) +
                            ":"  + ctx.lineMap().getColumnNumber(node.getStartPosition()));

                yield new CsvSrc(ctx.localFileLoader(), fileName, delimiter);
            }
            // TODO: at some point, make dfs nonnull
            case DFS -> new DfsSrc(Objects.requireNonNull(ctx.dfs(), "TODO: change this"), fileName);
        };
        return mst.getExpression().accept(this, ctx.withSrc(src).transitionState());
    }

    @Override
    public Program visitIdentifier(IdentifierTree node, Ctx ctx) {
        if(ctx.state != State.END)
            return throwIllegalState(node, ctx);

        if(!node.getName().equals("engine"))
            return throwIllegalState(node, ctx);

        return new Program(
                Objects.requireNonNull(ctx.src(), "Missing source"),
                ctx.partitions(),
                ctx.ops());
    }
}
