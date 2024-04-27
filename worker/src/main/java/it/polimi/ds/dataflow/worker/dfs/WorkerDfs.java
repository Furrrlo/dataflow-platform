package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.socket.packets.NetDfsFileInfo;
import org.jetbrains.annotations.Contract;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.SequencedCollection;

import static it.polimi.ds.dataflow.socket.packets.HelloPacket.PreviousJob;

public interface WorkerDfs extends Dfs {

    @Contract("null -> null; !null -> !null")
    default @Nullable DfsFile findFile(@Nullable NetDfsFileInfo netDfsFileInfo) {
        return netDfsFileInfo == null ? null : findFile(
                netDfsFileInfo.dfsFileName(),
                netDfsFileInfo.dfsFilePartitions(),
                netDfsFileInfo.dfsPartitionNames());
    }

    DfsFile findFile(String name, int partitions, Collection<String> allowedPartitionNames);

    @Nullable RestoredBackupInfo loadBackupInfo(int jobId, int partition, String dfsDstFileName);

    void writeBackupInfo(int jobId, int partition, DfsFilePartitionInfo dstFilePartition, @Nullable Integer nextBatchPtr);

    void writeBatchInPartitionAndBackup(int jobId,
                                        int srcPartition,
                                        DfsFilePartitionInfo dstFilePartition,
                                        Collection<Tuple2> tuple,
                                        @Nullable Integer nextBatchPtr);

    SequencedCollection<PreviousJob> readWorkerJobs();

    void deleteBackupAndFiles(int jobId, Collection<DfsFile> exclude);

    record RestoredBackupInfo(int jobId,
                              int partition,
                              DfsFilePartitionInfo dstFilePartition,
                              @Nullable Integer nextBatchPtr) {
    }
}
