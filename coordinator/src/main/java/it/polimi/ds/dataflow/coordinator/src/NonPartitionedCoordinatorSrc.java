package it.polimi.ds.dataflow.coordinator.src;

public sealed interface NonPartitionedCoordinatorSrc extends CoordinatorSrc permits CsvSrc, LinesSrc {

    int requestedPartitions();
}
