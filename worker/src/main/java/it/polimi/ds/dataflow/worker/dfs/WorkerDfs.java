package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.Dfs;
import it.polimi.ds.dataflow.dfs.DfsFile;
import org.jspecify.annotations.Nullable;

import java.util.Collection;

public interface WorkerDfs extends Dfs {

    void writeBackupInfo(int jobId, int partition, @Nullable Integer nextBatchPtr);

    void writeBatchAndBackup(int jobId,
                             int srcPartition,
                             DfsFile dstFile,
                             Collection<Tuple2> tuple,
                             @Nullable Integer nextBatchPtr);

    void writeBatchInPartitionAndBackup(int jobId,
                                        int srcPartition,
                                        DfsFile dstFile,
                                        int dstPartition,
                                        Collection<Tuple2> tuple,
                                        @Nullable Integer nextBatchPtr);

    void deleteBackup(int jobId, int partition);
}
