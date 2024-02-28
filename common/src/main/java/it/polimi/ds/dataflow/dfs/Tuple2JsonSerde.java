package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;

import java.io.UncheckedIOException;

public interface Tuple2JsonSerde {

    String jsonify(Tuple2 t) throws UncheckedIOException;

    Tuple2 parseJson(String json) throws UncheckedIOException;
}
