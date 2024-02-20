package it.polimi.ds.map_reduce.js;

public sealed interface CompiledOp permits
        FlatMapCompiledOp,
        MapCompiledOp,
        ChangeKeyCompiledOp,
        FilterCompiledOp,
        ReduceCompiledOp {
}
