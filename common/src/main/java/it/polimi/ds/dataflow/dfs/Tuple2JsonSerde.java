package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;

public interface Tuple2JsonSerde {

    default String jsonify(Tuple2 t) {
        return concatJson(jsonifyJsObj(t.key()), jsonifyJsObj(t.value()));
    }

    @SuppressFBWarnings("USBR_UNNECESSARY_STORE_BEFORE_RETURN") // See comment below
    default String concatJson(String keyJson, String valueJson) {
        // Need an additional var cause otherwise NullAway chokes on the analysis
        @SuppressWarnings("UnnecessaryLocalVariable")
        var str = STR."""
            {\
            "key":\{keyJson},\
            "value":\{valueJson},\
            }\
            """;
        return str;
    }

    String jsonifyJsObj(@Nullable Object t);

    Tuple2 parseJson(String json);
}
