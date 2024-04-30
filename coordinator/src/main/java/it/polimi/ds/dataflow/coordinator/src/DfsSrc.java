package it.polimi.ds.dataflow.coordinator.src;

import com.google.errorprone.annotations.MustBeClosed;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import org.jspecify.annotations.Nullable;

import java.util.stream.Stream;

public final class DfsSrc implements PartitionedCoordinatorSrc {

    private final CoordinatorDfs dfs;
    private final String fileName;
    private @Nullable DfsFile file;
    private final @Nullable String dstDfsFileName;

    public DfsSrc(CoordinatorDfs dfs, String fileName, @Nullable String dstDfsFileName) {
        this.dfs = dfs;
        this.fileName = fileName;
        this.dstDfsFileName = dstDfsFileName;
    }

    public DfsSrc(CoordinatorDfs dfs, DfsFile file, @Nullable String dstDfsFileName) {
        this.dfs = dfs;
        this.fileName = file.name();
        this.file = file;
        this.dstDfsFileName = dstDfsFileName;
    }

    @Override
    public DfsFile getDfsFile() {
        if(file == null)
            file = dfs.findFile(fileName);
        return file;
    }

    @Override
    public @Nullable String getRequestedDstDfsFileName() {
        return dstDfsFileName;
    }

    @Override
    public @MustBeClosed Stream<Tuple2> loadAll() {
        return dfs.loadAll(getDfsFile());
    }
}
