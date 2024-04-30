package it.polimi.ds.dataflow.coordinator.dfs;

import com.google.errorprone.annotations.MustBeClosed;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import org.jetbrains.annotations.Unmodifiable;

import java.util.SequencedCollection;
import java.util.stream.Stream;

public interface CoordinatorDfs extends Dfs {

    void validateFileName(String fileName);

    @Unmodifiable DfsFile createPartitionedFilePreemptively(String name, int partitions);

    @Unmodifiable DfsFile createPartitionedFile(String name, SequencedCollection<DfsFilePartitionInfo> partitions);

    DfsFile findFile(String name);

    @MustBeClosed Stream<Tuple2> loadAll(DfsFile file);

    void reshuffle(DfsFile file);
}
