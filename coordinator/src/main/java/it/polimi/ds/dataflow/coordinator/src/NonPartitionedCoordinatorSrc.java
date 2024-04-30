package it.polimi.ds.dataflow.coordinator.src;

import org.jspecify.annotations.Nullable;

public sealed interface NonPartitionedCoordinatorSrc extends CoordinatorSrc permits CsvSrc, LinesSrc {

    int requestedPartitions();

    @Nullable String requestedDstDfsFileName();

    @Nullable String requestedSrcDfsFileName();
}
