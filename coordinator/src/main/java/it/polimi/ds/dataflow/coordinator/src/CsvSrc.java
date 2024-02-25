package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;

import java.util.stream.Stream;

@SuppressWarnings({"FieldCanBeLocal", "unused"}) // TODO: remove
@SuppressFBWarnings("FCBL_FIELD_COULD_BE_LOCAL") // TODO: remove
public final class CsvSrc implements CoordinatorSrc {

    private final LocalSrcFileLoader loader;
    private final String fileName;
    private final @Nullable String delimiter;

    public CsvSrc(LocalSrcFileLoader loader, String fileName) {
        this(loader, fileName, null);
    }

    public CsvSrc(LocalSrcFileLoader loader, String fileName, @Nullable String delimiter) {
        this.loader = loader;
        this.fileName = fileName;
        this.delimiter = delimiter;
    }


    @Override
    public Stream<Tuple2> loadInitial() {
        throw new IllegalStateException("TODO");
    }
}
