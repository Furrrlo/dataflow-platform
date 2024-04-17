package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;
import org.jspecify.annotations.Nullable;

import java.util.function.BiFunction;
import java.util.function.Function;

public final class MapCompiledOp implements CompiledOp, Function<Tuple2, Tuple2> {

    private final BiFunction<Object, @Nullable Object, @Nullable Object> fn;

    public MapCompiledOp(BiFunction<Object, @Nullable Object, @Nullable Object> fn) {
        this.fn = fn;
    }

    @Override
    public Tuple2 apply(Tuple2 in) {
        return new Tuple2(in.key(), fn.apply(in.key(), in.value()));
    }
}
