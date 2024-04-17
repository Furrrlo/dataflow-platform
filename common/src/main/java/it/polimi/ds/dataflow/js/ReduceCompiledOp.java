package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.function.BiFunction;

public final class ReduceCompiledOp implements CompiledOp, BiFunction<Object, List<Object>, Tuple2> {

    private final BiFunction<Object, @Nullable Object, @Nullable Object> fn;

    public ReduceCompiledOp(BiFunction<Object, @Nullable Object, @Nullable Object> fn) {
        this.fn = fn;
    }

    @Override
    public Tuple2 apply(Object key, List<Object> values) {
        return new Tuple2(key, fn.apply(key, values));
    }
}
