package it.polimi.ds.map_reduce.dfs;

import org.jetbrains.annotations.Unmodifiable;

import java.util.SequencedCollection;

public record DfsFile(String name,
                      @Unmodifiable SequencedCollection<DfsFilePartitionInfo> partitions) {

    public int partitionsNum() {
        // TODO: this will most likely be incorrect on workers
        //       can we found it out somehow from postgres?
        return partitions.size();
    }
}
