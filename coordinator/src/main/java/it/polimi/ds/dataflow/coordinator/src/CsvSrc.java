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
    private final @Nullable String srcDfsFile;
    private final @Nullable String dstDfsFile;

    public CsvSrc(WorkDirFileLoader loader,
                  String fileName,
                  int partitions,
                  @Nullable String delimiter,
                  @Nullable String srcDfsFile,
                  @Nullable String dstDfsFile) {
        this.loader = loader;
        this.fileName = fileName;
        this.partitions = partitions;
        this.delimiter = delimiter;
        this.srcDfsFile = srcDfsFile;
        this.dstDfsFile = dstDfsFile;
    }

    @Override
    public Stream<Tuple2> loadAll() {
        throw new IllegalStateException("TODO");
    }

    @Override
    public int requestedPartitions() {
        return partitions;
    }

    @Override
    public @Nullable String requestedDstDfsFileName() {
        return srcDfsFile;
    }

    @Override
    public @Nullable String requestedSrcDfsFileName() {
        return dstDfsFile;
    }
}
