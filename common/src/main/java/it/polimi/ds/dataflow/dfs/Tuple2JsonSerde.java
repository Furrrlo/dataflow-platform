package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;

public interface Tuple2JsonSerde {

    String jsonify(Tuple2 t);

    Tuple2 parseJson(String json);
}
