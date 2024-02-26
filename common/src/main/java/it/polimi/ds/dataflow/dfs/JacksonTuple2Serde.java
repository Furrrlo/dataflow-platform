package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;

public class JacksonTuple2Serde implements Tuple2JsonSerde {

    @Override
    public String jsonify(Tuple2 t) {
        return ""; // TODO: write json
    }

    @Override
    public Tuple2 parseJson(String json) {
        return new Tuple2("", json); // TODO: parse json
    }
}
