package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.Tuple2;

import java.io.IOException;
import java.util.stream.Stream;

public sealed interface Src permits LinesSrc, CsvSrc {

    Stream<Tuple2> loadInitial(LocalSrcFileLoader loader) throws IOException;

    enum Kind {
        LINES("lines"), CSV("csv");

        private final String methodIdentifier;

        Kind(String methodIdentifier) {
            this.methodIdentifier = methodIdentifier;
        }

        public String getMethodIdentifier() {
            return methodIdentifier;
        }
    }
}
