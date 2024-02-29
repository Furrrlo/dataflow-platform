package it.polimi.ds.dataflow.src;

import com.google.errorprone.annotations.MustBeClosed;
import it.polimi.ds.dataflow.Tuple2;

import java.io.IOException;
import java.util.stream.Stream;

public interface Src {

    @MustBeClosed Stream<Tuple2> loadAll() throws IOException;
}
