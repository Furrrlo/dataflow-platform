package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.dfs.DfsFile;
import org.jspecify.annotations.Nullable;

public sealed interface PartitionedCoordinatorSrc extends CoordinatorSrc permits DfsSrc, RequireSrc {

    DfsFile getDfsFile();

    @Nullable String getRequestedDstDfsFileName();
}
