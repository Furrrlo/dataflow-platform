package it.polimi.ds.map_reduce.dfs;

import it.polimi.ds.map_reduce.Tuple2;
import org.jetbrains.annotations.Unmodifiable;

import java.io.Closeable;

public interface Dfs extends Closeable {

    void createFilePartition(String file, int partition);

    @Unmodifiable DfsFile findFile(String name);

    void write(DfsFile file, Tuple2 tuple);

    void writeInPartition(DfsFile file, Tuple2 tuple, int partition);
}
