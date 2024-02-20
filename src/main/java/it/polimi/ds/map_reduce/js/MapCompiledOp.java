package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.Tuple2;

import java.util.function.BiFunction;
import java.util.function.Function;

public final class MapCompiledOp implements CompiledOp, Function<Tuple2, Tuple2> {

    private final BiFunction<Object, Object, Object> fn;

    public MapCompiledOp(BiFunction<Object, Object, Object> fn) {
        this.fn = fn;
    }

    @Override
    public Tuple2 apply(Tuple2 in) {
        return new Tuple2(in.value(), fn.apply(in.key(), in.value()));
    }
}
