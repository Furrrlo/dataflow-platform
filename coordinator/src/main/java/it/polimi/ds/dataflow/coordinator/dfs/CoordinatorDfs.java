package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.dfs.Dfs;
import it.polimi.ds.map_reduce.dfs.DfsFile;
import org.jetbrains.annotations.Unmodifiable;

import java.util.stream.Stream;

public interface CoordinatorDfs extends Dfs {

    @Unmodifiable DfsFile createPartitionedFile(String name, int partitions);

    void write(DfsFile file, Stream<Tuple2> tuples);

    Stream<Tuple2> loadAll(DfsFile file);
}
