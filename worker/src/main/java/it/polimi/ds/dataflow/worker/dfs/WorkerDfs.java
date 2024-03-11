package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.SequencedCollection;

import static it.polimi.ds.dataflow.socket.packets.HelloPacket.PreviousJob;

public interface WorkerDfs extends Dfs {

    @Nullable RestoredBackupInfo loadBackupInfo(int jobId, int partition);

    void writeBackupInfo(int jobId, int partition, DfsFilePartitionInfo dstFilePartition, @Nullable Integer nextBatchPtr);

    void writeBatchInPartitionAndBackup(int jobId,
                                        int srcPartition,
                                        DfsFilePartitionInfo dstFilePartition,
                                        Collection<Tuple2> tuple,
                                        @Nullable Integer nextBatchPtr);

    SequencedCollection<PreviousJob> readWorkerJobs();

    void deleteBackup(int jobId, int partition);

    record RestoredBackupInfo(int jobId,
                              int partition,
                              DfsFilePartitionInfo dstFilePartition,
                              @Nullable Integer nextBatchPtr) {
    }
}
