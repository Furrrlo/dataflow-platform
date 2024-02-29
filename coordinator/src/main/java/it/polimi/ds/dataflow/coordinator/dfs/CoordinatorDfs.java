package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import org.jetbrains.annotations.Unmodifiable;

import java.util.stream.Stream;

public interface CoordinatorDfs extends Dfs {

    @Unmodifiable DfsFile createPartitionedFile(String name, int partitions);

    Stream<Tuple2> loadAll(DfsFile file);

    void reshuffle(DfsFile file);
}
