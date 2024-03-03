package it.polimi.ds.dataflow.worker.dfs;

import com.google.errorprone.annotations.MustBeClosed;
import it.polimi.ds.dataflow.dfs.Dfs;

import java.util.UUID;
import java.util.stream.Stream;

public interface WorkerDfs extends Dfs {
    void createBackupFile(String name);

    void updateBackupFile(String fileName, BackupInfo backupInfo);

    void writeBackupInfo(String fileName, BackupInfo backupInfo);

    void deleteBackupFile(String fileName, UUID toDelete);

    @MustBeClosed
    Stream<BackupInfo> loadAll(String name);

    String findFile(String name);

    record BackupInfo(UUID uuid, int jobId, int partition, Integer nextBatchPtr) {
    }
}
