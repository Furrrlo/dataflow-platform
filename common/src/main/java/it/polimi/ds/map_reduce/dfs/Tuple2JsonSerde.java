package it.polimi.ds.map_reduce.dfs;

import it.polimi.ds.map_reduce.Tuple2;

public interface Tuple2JsonSerde {

    String jsonify(Tuple2 t);

    Tuple2 parseJson(String json);
}
