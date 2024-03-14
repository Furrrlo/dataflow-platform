package it.polimi.ds.dataflow.utils;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import java.util.function.Function;

public interface SimpleScriptEngineFactory {

    ScriptEngine create() throws ScriptException;

    static <T extends ScriptEngineFactory> SimpleScriptEngineFactory wrap(T factory, Function<T, ScriptEngine> fn) {
        return () -> fn.apply(factory);
    }
}
