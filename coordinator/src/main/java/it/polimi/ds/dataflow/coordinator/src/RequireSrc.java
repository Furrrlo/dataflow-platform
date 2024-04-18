package it.polimi.ds.dataflow.coordinator.src;

import com.google.errorprone.annotations.MustBeClosed;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.DfsFile;

import java.util.stream.Stream;

public final class RequireSrc implements PartitionedCoordinatorSrc {

    @Override
    public @MustBeClosed Stream<Tuple2> loadAll() {
        throw new UnsupportedOperationException("Script requires src to be provided by another invoking script");
    }

    @Override
    public DfsFile getDfsFile() {
        throw new UnsupportedOperationException("Script requires src to be provided by another invoking script");
    }
}
