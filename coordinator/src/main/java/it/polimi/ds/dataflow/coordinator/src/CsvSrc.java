package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jspecify.annotations.Nullable;

import java.util.stream.Stream;

@SuppressWarnings({"FieldCanBeLocal", "unused"}) // TODO: remove
@SuppressFBWarnings("FCBL_FIELD_COULD_BE_LOCAL") // TODO: remove
public final class CsvSrc implements NonPartitionedCoordinatorSrc {

    private final WorkDirFileLoader loader;
    private final String fileName;
    private final int partitions;
    private final @Nullable String delimiter;

    public CsvSrc(WorkDirFileLoader loader, String fileName, int partitions, @Nullable String delimiter) {
        this.loader = loader;
        this.fileName = fileName;
        this.partitions = partitions;
        this.delimiter = delimiter;
    }

    @Override
    public Stream<Tuple2> loadAll() {
        throw new IllegalStateException("TODO");
    }

    @Override
    public int requestedPartitions() {
        return partitions;
    }
}
