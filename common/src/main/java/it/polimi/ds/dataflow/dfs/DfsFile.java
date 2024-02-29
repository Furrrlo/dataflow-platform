package it.polimi.ds.dataflow.dfs;

import org.jetbrains.annotations.Unmodifiable;

import java.util.SequencedCollection;

public record DfsFile(String name,
                      int partitionsNum,
                      @Unmodifiable SequencedCollection<DfsFilePartitionInfo> partitions) {

    public DfsFile(String name, @Unmodifiable SequencedCollection<DfsFilePartitionInfo> partitions) {
        this(name, partitions.size(), partitions);
    }
}
