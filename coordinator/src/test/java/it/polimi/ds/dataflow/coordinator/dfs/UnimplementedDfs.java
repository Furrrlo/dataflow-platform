package it.polimi.ds.dataflow.coordinator.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.CreateFileOptions;
import it.polimi.ds.dataflow.dfs.DfsFile;
import org.jetbrains.annotations.Unmodifiable;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.stream.Stream;

public class UnimplementedDfs implements CoordinatorDfs {

    @Override
    public void createFilePartition(String file, int partition, CreateFileOptions... options) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public @Unmodifiable DfsFile findFile(String name) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void write(DfsFile file, Tuple2 tuple) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeInPartition(DfsFile file, Tuple2 tuple, int partition) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeBatch(DfsFile file, Collection<Tuple2> tuple) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeBatchInPartition(DfsFile file, int partition, Collection<Tuple2> tuple) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public BatchRead readNextBatch(DfsFile file, int partition, int batchHint, @Nullable Integer nextBatchPtr) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public @Unmodifiable DfsFile createPartitionedFile(String name, int partitions) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Stream<Tuple2> loadAll(DfsFile file) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void reshuffle(DfsFile file) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
