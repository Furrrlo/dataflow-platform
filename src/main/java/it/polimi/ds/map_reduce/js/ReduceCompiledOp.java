package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.Tuple2;

import java.util.List;
import java.util.function.BiFunction;

public final class ReduceCompiledOp implements CompiledOp, BiFunction<Object, List<Object>, Tuple2> {

    private final BiFunction<Object, Object, Object> fn;

    public ReduceCompiledOp(BiFunction<Object, Object, Object> fn) {
        this.fn = fn;
    }

    @Override
    public Tuple2 apply(Object key, List<Object> values) {
        return new Tuple2(key, fn.apply(key, values));
    }
}
