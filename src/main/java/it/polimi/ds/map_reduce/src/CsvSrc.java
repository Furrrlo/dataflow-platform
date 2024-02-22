package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;

import java.util.stream.Stream;

@SuppressWarnings({"FieldCanBeLocal", "unused"}) // TODO: remove
@SuppressFBWarnings("FCBL_FIELD_COULD_BE_LOCAL") // TODO: remove
public final class CsvSrc implements Src {

    private final String fileName;
    private final @Nullable String delimiter;

    public CsvSrc(String fileName) {
        this(fileName, null);
    }

    public CsvSrc(String fileName, @Nullable String delimiter) {
        this.fileName = fileName;
        this.delimiter = delimiter;
    }


    @Override
    public Stream<Tuple2> loadInitial(LocalSrcFileLoader loader) {
        throw new IllegalStateException("TODO");
    }
}
