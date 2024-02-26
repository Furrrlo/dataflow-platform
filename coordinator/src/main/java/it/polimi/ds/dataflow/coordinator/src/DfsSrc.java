package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.coordinator.dfs.CoordinatorDfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.Tuple2;
import org.jspecify.annotations.Nullable;

import java.util.stream.Stream;

public final class DfsSrc implements CoordinatorSrc {

    private final CoordinatorDfs dfs;
    private final String fileName;
    private @Nullable DfsFile file;

    public DfsSrc(CoordinatorDfs dfs, String fileName) {
        this.dfs = dfs;
        this.fileName = fileName;
    }

    public DfsSrc(CoordinatorDfs dfs, DfsFile file) {
        this.dfs = dfs;
        this.fileName = file.name();
        this.file = file;
    }

    @Override
    public Stream<Tuple2> loadAll() {
        if(file == null)
            file = dfs.findFile(fileName);
        return dfs.loadAll(file);
    }
}
