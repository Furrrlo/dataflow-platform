package it.polimi.ds.dataflow.worker.dfs;

import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;

import java.util.SequencedCollection;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.worker.dfs.Tables.DATAFLOW_JOBS;

public record TestBackupInfo(int jobId, int partition, @Nullable Integer nextBatchPtr) {

    public static void truncate(DSLContext ctx) {
        ctx.truncate(DATAFLOW_JOBS).execute();
    }

    public static Stream<TestBackupInfo> loadAllFrom(DSLContext ctx) {
        return ctx.select(DATAFLOW_JOBS.WORKER, DATAFLOW_JOBS.JOBID, DATAFLOW_JOBS.PARTITION, DATAFLOW_JOBS.NEXTBATCHPTR)
                .from(DATAFLOW_JOBS)
                .stream()
                .map(r -> new TestBackupInfo(
                        r.get(DATAFLOW_JOBS.JOBID),
                        r.get(DATAFLOW_JOBS.PARTITION),
                        r.get(DATAFLOW_JOBS.NEXTBATCHPTR)
                ));
    }

    public static void writeMultipleBackupInfos(WorkerDfs dfs, SequencedCollection<TestBackupInfo> backups) {
        backups.forEach(
                b -> dfs.writeBackupInfo(
                        b.jobId,
                        b.partition,
                        b.nextBatchPtr));
    }
}
