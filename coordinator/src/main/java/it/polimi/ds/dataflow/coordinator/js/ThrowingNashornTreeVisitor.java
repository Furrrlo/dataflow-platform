package it.polimi.ds.dataflow.coordinator.js;

import org.openjdk.nashorn.api.tree.*;

public class ThrowingNashornTreeVisitor<R, P> implements TreeVisitor<R, P> {

    protected R throwIllegalState(Tree node, P p) {
        throw new IllegalStateException();
    }

    @Override
    public R visitUnknown(Tree node, P p) {
        throw new UnknownTreeException(node, p);
    }

    // Everything else throws

    @Override
    public R visitAssignment(AssignmentTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitCompoundAssignment(CompoundAssignmentTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitBinary(BinaryTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitBlock(BlockTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitBreak(BreakTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitCase(CaseTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitCatch(CatchTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitClassDeclaration(ClassDeclarationTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitClassExpression(ClassExpressionTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitConditionalExpression(ConditionalExpressionTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitContinue(ContinueTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitDebugger(DebuggerTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitDoWhileLoop(DoWhileLoopTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitErroneous(ErroneousTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitExpressionStatement(ExpressionStatementTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitForLoop(ForLoopTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitForInLoop(ForInLoopTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitForOfLoop(ForOfLoopTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitFunctionCall(FunctionCallTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitFunctionDeclaration(FunctionDeclarationTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitFunctionExpression(FunctionExpressionTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitIdentifier(IdentifierTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitIf(IfTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitArrayAccess(ArrayAccessTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitArrayLiteral(ArrayLiteralTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitLabeledStatement(LabeledStatementTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitLiteral(LiteralTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitParenthesized(ParenthesizedTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitReturn(ReturnTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitMemberSelect(MemberSelectTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitNew(NewTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitObjectLiteral(ObjectLiteralTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitProperty(PropertyTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitRegExpLiteral(RegExpLiteralTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitTemplateLiteral(TemplateLiteralTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitEmptyStatement(EmptyStatementTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitSpread(SpreadTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitSwitch(SwitchTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitThrow(ThrowTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitCompilationUnit(CompilationUnitTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitModule(ModuleTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitExportEntry(ExportEntryTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitImportEntry(ImportEntryTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitTry(TryTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitInstanceOf(InstanceOfTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitUnary(UnaryTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitVariable(VariableTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitWhileLoop(WhileLoopTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitWith(WithTree node, P p) {
        return throwIllegalState(node, p);
    }

    @Override
    public R visitYield(YieldTree node, P p) {
        return throwIllegalState(node, p);
    }
}
