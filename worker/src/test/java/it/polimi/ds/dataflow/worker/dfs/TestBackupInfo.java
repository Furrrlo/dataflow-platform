package it.polimi.ds.dataflow.worker.dfs;

import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;

import java.util.SequencedCollection;
import java.util.UUID;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.worker.dfs.Tables.DATAFLOW_JOBS;

public record TestBackupInfo(
        UUID uuid,
        int jobId,
        int partition,
        String partitionFileName,
        @Nullable Integer nextBatchPtr
) {

    public static void truncate(DSLContext ctx) {
        ctx.truncate(DATAFLOW_JOBS).execute();
    }

    public static Stream<TestBackupInfo> loadAllFrom(DSLContext ctx) {
        return ctx.select(DATAFLOW_JOBS.WORKER, DATAFLOW_JOBS.JOBID, DATAFLOW_JOBS.PARTITION, DATAFLOW_JOBS.DSTFILEPARTITION, DATAFLOW_JOBS.NEXTBATCHPTR)
                .from(DATAFLOW_JOBS)
                .stream()
                .map(r -> new TestBackupInfo(
                        r.get(DATAFLOW_JOBS.WORKER),
                        r.get(DATAFLOW_JOBS.JOBID),
                        r.get(DATAFLOW_JOBS.PARTITION),
                        r.get(DATAFLOW_JOBS.DSTFILEPARTITION),
                        r.get(DATAFLOW_JOBS.NEXTBATCHPTR)
                ));
    }

    public static void writeMultipleBackupInfos(WorkerDfs dfs, SequencedCollection<TestBackupInfo> backups) {
        backups.forEach(
                b -> dfs.writeBackupInfo(
                        b.jobId,
                        b.partition,
                        new DfsFilePartitionInfo(
                                "", b.partitionFileName, b.partition, "localhost", true),
                        b.nextBatchPtr));
    }
}
