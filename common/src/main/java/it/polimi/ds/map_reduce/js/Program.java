package it.polimi.ds.map_reduce.js;

import it.polimi.ds.map_reduce.src.Src;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.openjdk.nashorn.api.scripting.JSObject;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;

public record Program(Src src, int partitions, List<Op> ops) {

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification =  "Can't make it more general 'cause it's the canonical ctor")
    public Program(Src src, int partitions, List<Op> ops) {
        if(ops.stream().limit(ops.size() - 1).anyMatch(o -> o.kind().isTerminal()))
            throw new IllegalArgumentException("Intermediate instruction is terminal");

        this.src = src;
        this.partitions = partitions;
        this.ops = List.copyOf(ops);
    }

    public CompiledProgram compile(ScriptEngine engine) throws ScriptException {
        List<CompiledOp> ops = new ArrayList<>(this.ops.size());
        for (Op op : this.ops) {
            ops.add(compile(engine, op));
        }
        return new CompiledProgram(src, partitions, ops);
    }

    public static List<CompiledOp> compile(ScriptEngine engine, List<Op> ops) throws ScriptException {
        List<CompiledOp> res = new ArrayList<>(ops.size());
        for (Op op : ops) {
            res.add(compile(engine, op));
        }
        return res;
    }

    @SuppressFBWarnings(
            value = "VA_FORMAT_STRING_USES_NEWLINE",
            justification = "Not printed, only compiled and hard to change 'cause it's a multiline string")
    public static CompiledOp compile(ScriptEngine engine, Op op) throws ScriptException {
        // Replace the global object, just in case it's fucked
        engine.setBindings(engine.createBindings(), ScriptContext.GLOBAL_SCOPE);

        Object eval = switch (op.kind()) {
            // Double negation to cast to boolean
            case FILTER -> engine.eval("(function(k, v) { return !!(" + op.body() + ")(k, v); })");
            // Convert Map to a [[K, V]] array, as I can't seem to extract the first on the java side
            case FLAT_MAP -> engine.eval(String.format(
                    Locale.ROOT,
                    """
                    (function(k, v) {
                        let map = (%s)(k, v);
                        if('entries' in map) {
                            let arr = [];
                            for(const e of map.entries()) { arr.push(e); }
                            return arr;
                        }
                        return map;
                    })""", op.body()));
            // Convert from a Java collection to a JS one, don't know why it's not automatic
            case REDUCE -> engine.eval("(function(k, v) { return (" + op.body() + ")(k, Java.from(v)); })");
            default -> engine.eval(op.body());
        };

        if(!(eval instanceof JSObject obj) || !obj.isFunction())
            throw new IllegalStateException("Compiled function is not a function");

        @SuppressWarnings("unchecked")
        BiFunction<Object, Object, Object> fn = obj instanceof ScriptObjectMirror mirror
                ? mirror.to(BiFunction.class)
                : (k, v) -> obj.call(null, k, v);
        return switch (op.kind()) {
            case FLAT_MAP -> new FlatMapCompiledOp(fn);
            case MAP -> new MapCompiledOp(fn);
            case FILTER -> new FilterCompiledOp(fn);
            case CHANGE_KEY -> new ChangeKeyCompiledOp(fn);
            case REDUCE -> new ReduceCompiledOp(fn);
        };
    }
}
