package it.polimi.ds.dataflow.dfs;

import it.polimi.ds.dataflow.Tuple2;
import org.jetbrains.annotations.Unmodifiable;
import org.jspecify.annotations.Nullable;

import java.io.Closeable;
import java.util.Collection;

public interface Dfs extends Closeable {

    void createFilePartition(String file, int partition, CreateFileOptions... options);

    DfsFile findFile(String name);

    DfsFile findFile(String name, int partitions);

    void write(DfsFile file, Tuple2 tuple);

    void writeInPartition(DfsFile file, Tuple2 tuple, int partition);

    void writeBatch(DfsFile file, Collection<Tuple2> tuple);

    void writeBatchInPartition(DfsFile file, int partition, Collection<Tuple2> tuple);

    BatchRead readNextBatch(DfsFile file, int partition, int batchHint, @Nullable Integer nextBatchPtr);

    record BatchRead(@Nullable Integer nextBatchPtr, @Unmodifiable Collection<Tuple2> data) {
    }
}
