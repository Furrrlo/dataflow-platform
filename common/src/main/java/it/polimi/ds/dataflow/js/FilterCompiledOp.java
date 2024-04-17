package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Predicate;

public final class FilterCompiledOp implements CompiledOp, Predicate<Tuple2> {

    private final BiFunction<Object, @Nullable Object, @Nullable Object> fn;

    public FilterCompiledOp(BiFunction<Object, @Nullable Object, @Nullable Object> fn) {
        this.fn = fn;
    }

    @Override
    public boolean test(Tuple2 in) {
        final Object res = fn.apply(in.key(), in.value());
        if(!(res instanceof Boolean bool))
            throw new IllegalStateException("Invalid return type for filter op " + res);
        return bool;
    }
}
