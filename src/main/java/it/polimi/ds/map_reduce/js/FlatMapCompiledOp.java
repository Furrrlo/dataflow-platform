package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.Tuple2;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FlatMapCompiledOp implements CompiledOp, Function<Tuple2, Map<Object, Object>> {

    private final BiFunction<Object, Object, Object> fn;

    public FlatMapCompiledOp(BiFunction<Object, Object, Object> fn) {
        this.fn = fn;
    }

    @Override
    public Map<Object, Object> apply(Tuple2 in) {
        final Object res = fn.apply(in.key(), in.value());
        if(res == null)
            return Map.of();

        if(!(res instanceof ScriptObjectMirror obj) || !obj.isArray())
            throw new IllegalStateException("Invalid return type for flatMap op " + res);

        // So here we have a [[K, V]], so a List of 2-element-lists (respectively key first el and value second)
        // Convert it back to a map
        return ((List<?>) obj.to(List.class)).stream().map(el -> {
            if(!(el instanceof ScriptObjectMirror elObj) || !elObj.isArray())
                throw new IllegalStateException("Invalid returned element for flatMap op " + el);
            return (List<?>) elObj.to(List.class);
        }).collect(Collectors.toMap(
                ls -> {
                    if(ls.size() != 2)
                        throw new IllegalStateException("Invalid returned element for flatMap op " + ls);
                    return ls.get(0);
                },
                ls -> ls.get(1))
        );
    }
}
