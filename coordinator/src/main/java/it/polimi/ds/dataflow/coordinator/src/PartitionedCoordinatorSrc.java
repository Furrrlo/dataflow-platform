package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.dfs.DfsFile;

public sealed interface PartitionedCoordinatorSrc extends CoordinatorSrc permits DfsSrc, RequireSrc {

    DfsFile getDfsFile();
}
