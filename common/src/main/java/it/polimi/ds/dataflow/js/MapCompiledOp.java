package it.polimi.ds.dataflow.js;

import it.polimi.ds.dataflow.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

public final class MapCompiledOp implements CompiledOp, Function<Tuple2, Tuple2> {

    private final BiFunction<Object, Object, Object> fn;

    public MapCompiledOp(BiFunction<Object, Object, Object> fn) {
        this.fn = fn;
    }

    @Override
    public Tuple2 apply(Tuple2 in) {
        return new Tuple2(in.key(), fn.apply(in.key(), in.value()));
    }
}
