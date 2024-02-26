package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.Tuple2;

import java.io.IOException;
import java.util.stream.Stream;

public interface Src {

    boolean isNonPartitioned();

    Stream<Tuple2> loadAll() throws IOException;
}
