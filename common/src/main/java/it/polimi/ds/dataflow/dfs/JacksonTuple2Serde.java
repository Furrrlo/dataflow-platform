package it.polimi.ds.dataflow.dfs;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import it.polimi.ds.dataflow.Tuple2;
import org.jspecify.annotations.Nullable;
import org.openjdk.nashorn.api.scripting.JSObject;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("serial")
public class JacksonTuple2Serde implements Tuple2JsonSerde {

    private final ObjectMapper mapper;

    public JacksonTuple2Serde(ScriptEngine engine) throws ScriptException {
        JsonFactory factory = JsonFactory.builder()
                .recyclerPool(JsonRecyclerPools.sharedLockFreePool())
                .build();
        this.mapper = new ObjectMapper(factory)
                .registerModule(new SimpleModule()
                        .addSerializer(Tuple2.class, new Tuple2Serializer())
                        .addDeserializer(Tuple2.class, new Tuple2Deserializer(engine))
                        .addSerializer(ScriptObjectMirror.class, new ScriptObjectMirrorSerializer())
                        .addDeserializer(ScriptObjectMirror.class, new ScriptObjectMirrorDeserializer(engine)));
    }

    @Override
    public String jsonify(Tuple2 t) {
        try {
            return mapper.writeValueAsString(t);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to write to json", e);
        }
    }

    @Override
    public Tuple2 parseJson(String json) {
        try {
            return mapper.readValue(json, Tuple2.class);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to read json", e);
        }
    }

    private static class Tuple2Serializer extends StdSerializer<Tuple2> {

        protected Tuple2Serializer() {
            super((Class<Tuple2>) null);
        }

        @Override
        public void serialize(Tuple2 value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();
            gen.writeObjectField("key", value.key());
            gen.writeObjectField("value", value.value());
            gen.writeEndObject();
        }
    }

    private static class Tuple2Deserializer extends StdDeserializer<Tuple2> {

        private final Function<Object, Object> javaListToJsArrFn;

        @SuppressWarnings("unchecked")
        protected Tuple2Deserializer(ScriptEngine engine) throws ScriptException {
            super((Class<?>) null);

            var javaListToJsArrObj = engine.eval("(function(arr) { return Java.from(arr); })");
            this.javaListToJsArrFn = javaListToJsArrObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Function.class)
                    : m -> ((JSObject) javaListToJsArrObj).call(null, m);
        }

        @Override
        public Tuple2 deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            return new Tuple2(
                    Objects.requireNonNull(
                            ScriptObjectMirrorDeserializer.readTreeAsAny(ctxt, javaListToJsArrFn, node.get("key")),
                            "null key shouldn't be allowed"),
                    ScriptObjectMirrorDeserializer.readTreeAsAny(ctxt, javaListToJsArrFn, node.get("value")));
        }
    }

    private static class ScriptObjectMirrorSerializer extends StdSerializer<ScriptObjectMirror> {

        protected ScriptObjectMirrorSerializer() {
            super((Class<ScriptObjectMirror>) null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void serialize(ScriptObjectMirror value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            if(value.isArray()) {
                gen.writeObject(value.to(List.class));
                return;
            }

            gen.writeStartObject();
            switch (value.getClassName()) {
                case "Map", "WeakMap" -> {
                    var toArrayFnObj = value.eval("""
                            (function(map) {
                                let arr = [];
                                for(const e of map.entries()) { arr.push(e); }
                                return arr;
                            })
                            """);
                    Function<Object, Object> toArrayFn = toArrayFnObj instanceof ScriptObjectMirror mirror
                            ? mirror.to(Function.class)
                            : m -> ((JSObject) toArrayFnObj).call(null, m);

                    gen.writeStringField("jsObjectType", value.getClassName());
                    gen.writeObjectField("map", ((ScriptObjectMirror) toArrayFn.apply(value)).to(List.class));
                }
                case "Set", "WeakSet" -> {
                    var toArrayFnObj = value.eval("""
                            (function(set) {
                                let arr = [];
                                for(const e of set.values()) { arr.push(e); }
                                return arr;
                            })
                            """);
                    Function<Object, Object> toArrayFn = toArrayFnObj instanceof ScriptObjectMirror mirror
                            ? mirror.to(Function.class)
                            : m -> ((JSObject) toArrayFnObj).call(null, m);

                    gen.writeStringField("jsObjectType", value.getClassName());
                    gen.writeObjectField("set", ((ScriptObjectMirror) toArrayFn.apply(value)).to(List.class));
                }
                case "Date" -> {
                    gen.writeStringField("jsObjectType", value.getClassName());
                    gen.writeStringField("date", (String) value.callMember("toISOString"));
                }
                default -> {
                    gen.writeStringField("jsObjectType", "Object");

                    gen.writeFieldName("obj");
                    gen.writeStartObject();
                    for (Map.Entry<String, Object> e : value.entrySet()) {
                        if(!e.getKey().equals("__proto__"))
                            gen.writeObjectField(e.getKey(), e.getValue());
                    }
                    gen.writeEndObject();
                }
            }
            gen.writeEndObject();
        }
    }

    private static class ScriptObjectMirrorDeserializer extends StdDeserializer<ScriptObjectMirror> {

        private final Function<Object, Object> javaListToJsArrFn;
        private final Function<Object, Object> javaListToMapFn;
        private final Function<Object, Object> javaListToSetFn;
        private final Function<Object, Object> isoStringToDateFn;
        private final Supplier<Object> newObjectFn;

        @SuppressWarnings("unchecked")
        protected ScriptObjectMirrorDeserializer(ScriptEngine engine) throws ScriptException {
            super((Class<?>) null);

            var javaListToJsArrObj = engine.eval("(function(arr) { return Java.from(arr); })");
            this.javaListToJsArrFn = javaListToJsArrObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Function.class)
                    : m -> ((JSObject) javaListToJsArrObj).call(null, m);

            var javaListToMapObj = engine.eval("(function(arr) { return new Map(Java.from(arr)); })");
            this.javaListToMapFn = javaListToMapObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Function.class)
                    : m -> ((JSObject) javaListToMapObj).call(null, m);

            var javaListToSetObj = engine.eval("(function(arr) { return new Set(Java.from(arr)); })");
            this.javaListToSetFn = javaListToSetObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Function.class)
                    : m -> ((JSObject) javaListToSetObj).call(null, m);

            var isoStringToDateObj = engine.eval("(function(str) { return new Date(str); })");
            this.isoStringToDateFn = isoStringToDateObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Function.class)
                    : m -> ((JSObject) isoStringToDateObj).call(null, m);

            var newObjectObj = engine.eval("(function() { return {}; })");
            this.newObjectFn = newObjectObj instanceof ScriptObjectMirror mirror
                    ? mirror.to(Supplier.class)
                    : () -> ((JSObject) newObjectObj).call(null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public ScriptObjectMirror deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);
            String jsObjectType = (node.get("jsObjectType") != null) ? node.get("jsObjectType").asText() : "Object";
            return switch (jsObjectType) {
                case "Map", "WeakMap" -> {
                    List<Object> list = ctxt.readTreeAsValue(node.get("map"), List.class);
                    yield (ScriptObjectMirror) javaListToMapFn.apply(list);
                }
                case "Set", "WeakSet" -> {
                    List<Object> list = ctxt.readTreeAsValue(node.get("set"), List.class);
                    yield (ScriptObjectMirror) javaListToSetFn.apply(list);
                }
                case "Date" -> {
                    String str = node.get("date").asText();
                    yield (ScriptObjectMirror) isoStringToDateFn.apply(str);
                }
                case "Object" -> {
                    ScriptObjectMirror obj = (ScriptObjectMirror) newObjectFn.get();
                    for (var it = node.get("obj").fields(); it.hasNext(); ) {
                        var e = it.next();
                        obj.setMember(e.getKey(), readTreeAsAny(ctxt, javaListToJsArrFn, e.getValue()));
                    }

                    yield obj;
                }
                default -> throw new UnsupportedOperationException("Unrecognized js object type " + jsObjectType);
            };
        }


        public static @Nullable Object readTreeAsAny(DeserializationContext ctxt,
                                                     Function<Object, Object> javaListToJsArrFn,
                                                     JsonNode node) throws IOException {
            return switch (node.getNodeType()) {
                case STRING -> node.textValue();
                case NUMBER -> node.numberValue();
                case BOOLEAN -> node.asBoolean();
                case MISSING, NULL -> null;
                case BINARY -> node.binaryValue();
                case ARRAY -> {
                    var list = new ArrayList<>();
                    for (Iterator<JsonNode> it = node.elements(); it.hasNext(); )
                        list.add(readTreeAsAny(ctxt, javaListToJsArrFn, it.next()));
                    yield javaListToJsArrFn.apply(list);
                }
                case OBJECT -> ctxt.readTreeAsValue(node, ScriptObjectMirror.class);
                case POJO -> {
                    if(node instanceof POJONode pojoNode)
                        yield pojoNode.getPojo();

                    throw new AssertionError("Jackson node of type POJO was not a POJONode " + node);
                }
            };
        }
    }
}
