package it.polimi.ds.dataflow.js;

public sealed interface CompiledOp permits
        FlatMapCompiledOp,
        MapCompiledOp,
        ChangeKeyCompiledOp,
        FilterCompiledOp,
        ReduceCompiledOp {
}
