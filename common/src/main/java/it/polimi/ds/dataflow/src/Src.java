package it.polimi.ds.dataflow.src;

import it.polimi.ds.dataflow.Tuple2;

import java.io.IOException;
import java.util.stream.Stream;

public interface Src {

    Stream<Tuple2> loadAll() throws IOException;
}
