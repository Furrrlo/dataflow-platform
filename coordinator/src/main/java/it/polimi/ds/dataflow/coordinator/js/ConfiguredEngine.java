package it.polimi.ds.dataflow.coordinator.js;

import org.openjdk.nashorn.api.scripting.JSObject;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ConfiguredEngine {

    static ConfiguredEngine runFor(ScriptEngine engine, String setupScript) throws ScriptException {
        Object eval = engine.eval(setupScript);

        if(!(eval instanceof JSObject obj) || !obj.isFunction())
            throw new IllegalStateException("Compiled setup function is not a function");

        var inputConfiguredEngine = new ConfiguredEngine();

        Object returnedConfiguredEngine = obj.call(null, inputConfiguredEngine);
        if(returnedConfiguredEngine != inputConfiguredEngine)
            throw new IllegalStateException("Setup function does not return the engine passed as input");

        return inputConfiguredEngine;
    }

    private final Map<String, Object> vars = new LinkedHashMap<>();

    private ConfiguredEngine() {
    }

    @SuppressWarnings("unused") // Called from javascript
    public ConfiguredEngine declareVar(String name, Object value) {
        if(!(value instanceof String) && !(value instanceof Boolean) && !(value instanceof Number))
            throw new UnsupportedOperationException("Only string & primitives can be used in engineVars");

        vars.put(name, value);
        return this;
    }

    Map<String, Object> getVars() {
        return vars;
    }
}
