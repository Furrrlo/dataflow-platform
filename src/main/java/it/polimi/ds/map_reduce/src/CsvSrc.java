package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.Tuple2;

import java.util.stream.Stream;

public final class CsvSrc implements Src {

    private final String fileName;
    private final String delimiter;

    public CsvSrc(String fileName) {
        this(fileName, null);
    }

    public CsvSrc(String fileName, String delimiter) {
        this.fileName = fileName;
        this.delimiter = delimiter;
    }


    @Override
    public Stream<Tuple2> loadInitial(LocalSrcFileLoader loader) {
        throw new IllegalStateException("TODO");
    }
}
