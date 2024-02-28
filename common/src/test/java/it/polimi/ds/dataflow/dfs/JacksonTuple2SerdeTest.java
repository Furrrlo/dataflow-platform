package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;
import org.junit.jupiter.api.Test;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JacksonTuple2SerdeTest {

    static final ScriptEngine ENGINE = new NashornScriptEngineFactory().getScriptEngine("--language=es6", "-doe");
    static final JacksonTuple2Serde SERDE;
    static {
        try {
            SERDE = new JacksonTuple2Serde(ENGINE);
        } catch (ScriptException e) {
            throw new RuntimeException("Failed to init serde", e);
        }
    }

    @Test
    void serdeArray() throws ScriptException {
        var arr = ENGINE.eval("[ 1, 2, 3, 4, 5 ]");
        var tuple = new Tuple2(arr, arr);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(
                ((ScriptObjectMirror) arr).to(List.class),
                ((ScriptObjectMirror) Objects.requireNonNull(res.key())).to(List.class));
        assertEquals(
                ((ScriptObjectMirror) arr).to(List.class),
                ((ScriptObjectMirror) Objects.requireNonNull(res.value())).to(List.class));
    }

    @Test
    void serdeDate() throws ScriptException {
        var arr = ENGINE.eval("new Date()");
        var tuple = new Tuple2(arr, arr);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(
                ((ScriptObjectMirror) arr).callMember("toUTCString"),
                ((ScriptObjectMirror) Objects.requireNonNull(res.key())).callMember("toUTCString"));
        assertEquals(
                ((ScriptObjectMirror) arr).callMember("toUTCString"),
                ((ScriptObjectMirror) Objects.requireNonNull(res.value())).callMember("toUTCString"));
    }

    @Test
    void serdeMap() throws ScriptException {
        var arr = ENGINE.eval("new Map([ [1, 2], ['ciao', 4] ])");
        var tuple = new Tuple2(arr, arr);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        System.out.println(json);
        // TODO: assert
    }

    @Test
    void serdeSet() throws ScriptException {
        var arr = ENGINE.eval("new Set([ 1, 2, 3, 4, 5 ])");
        var tuple = new Tuple2(arr, arr);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        System.out.println(json);
        // TODO: assert
    }

    @Test
    void serdeObject() throws ScriptException {
        var obj = ENGINE.eval("(function() { return { a: 1, b: '2' }; })()");
        var tuple = new Tuple2(obj, obj);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        System.out.println(json);
        // TODO: assert
    }

    @Test
    void serdeString() throws ScriptException {
        var str = ENGINE.eval("\"stringa\"");
        var tuple = new Tuple2(str, str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, Objects.requireNonNull(res.key()));
        assertEquals(str, Objects.requireNonNull(res.value()));
    }

    @Test
    void serdeInt() throws ScriptException {
        var str = ENGINE.eval("1");
        var tuple = new Tuple2(str, str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, Objects.requireNonNull(res.key()));
        assertEquals(str, Objects.requireNonNull(res.value()));
    }

    @Test
    void serdeDouble() throws ScriptException {
        var str = ENGINE.eval("1.0");
        var tuple = new Tuple2(str, str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, Objects.requireNonNull(res.key()));
        assertEquals(str, Objects.requireNonNull(res.value()));
    }

    @Test
    void serdeTrue() throws ScriptException {
        var str = ENGINE.eval("true");
        var tuple = new Tuple2(str, str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, Objects.requireNonNull(res.key()));
        assertEquals(str, Objects.requireNonNull(res.value()));
    }

    @Test
    void serdeFalse() throws ScriptException {
        var str = ENGINE.eval("false");
        var tuple = new Tuple2(str, str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, Objects.requireNonNull(res.key()));
        assertEquals(str, Objects.requireNonNull(res.value()));
    }

    @Test
    void serdeNull() throws ScriptException {
        var str = ENGINE.eval("null");
        var tuple = new Tuple2("", str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, res.value());
    }

    @Test
    void serdeUndefined() throws ScriptException {
        var str = ENGINE.eval("undefined");
        var tuple = new Tuple2("", str);
        var json = SERDE.jsonify(tuple);
        var res = SERDE.parseJson(json);
        assertEquals(str, res.value());
    }
}