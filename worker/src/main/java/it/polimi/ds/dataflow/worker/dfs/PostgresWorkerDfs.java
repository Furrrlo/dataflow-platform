package it.polimi.ds.dataflow.worker.dfs;

import com.zaxxer.hikari.HikariConfig;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.*;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.lockservice.LockServiceFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.jooq.DSLContext;
import org.jspecify.annotations.Nullable;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Collection;
import java.util.SequencedCollection;
import java.util.UUID;
import java.util.function.Consumer;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.FOREIGN;
import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.IF_NOT_EXISTS;
import static it.polimi.ds.dataflow.socket.packets.HelloPacket.PreviousJob;
import static it.polimi.ds.dataflow.worker.dfs.Tables.DATAFLOW_JOBS;
import static org.jooq.impl.DSL.field;

public class PostgresWorkerDfs extends PostgresDfs implements WorkerDfs {

    private final UUID uuid;
    private final String coordinatorName;

    public PostgresWorkerDfs(ScriptEngine engine,
                             String coordinatorName,
                             UUID uuid,
                             Consumer<HikariConfig> configurator) throws ScriptException {
        super(engine, configurator);
        this.coordinatorName = coordinatorName;
        this.uuid = uuid;
        performLiquibaseMigration();
    }

    public PostgresWorkerDfs(String coordinatorName,
                             UUID uuid,
                             Tuple2JsonSerde serde,
                             Consumer<HikariConfig> configurator) {
        super(serde, configurator);
        this.coordinatorName = coordinatorName;
        this.uuid = uuid;
        performLiquibaseMigration();
    }

    @SuppressWarnings("deprecation")
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS") // Shouldn't happen anyway
    private void performLiquibaseMigration() {
        try (Liquibase liquibase = createLiquibase()) {
            var lockService = LockServiceFactory.getInstance().getLockService(liquibase.getDatabase());
            lockService.waitForLock();
            try {
                liquibase.validate();
                liquibase.update();
            } finally {
                lockService.releaseLock();
            }
        } catch (LiquibaseException ex) {
            throw new IllegalStateException("Failed to perform Liquibase migration", ex);
        }
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS") // Shouldn't happen anyway
    private Liquibase createLiquibase() {
        try (ClassLoaderResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor()) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(dataSource.getConnection()));
            return new Liquibase(
                    getClass().getPackageName().replace('.', '/') + "/changelog-root.xml",
                    resourceAccessor, database);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create Liquibase object", ex);
        }
    }

    @Override
    protected DfsFilePartitionInfo doCreateFilePartition(String file,
                                                         String partitionFile,
                                                         int partition,
                                                         CreateFileOptions... options) {
        ctx.transaction(tx -> {
            // CREATE TABLE IF NOT EXISTS may fail in concurrent settings
            // with a unique index violation on some internal pg index
            // See https://www.postgresql.org/message-id/CA+TgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg@mail.gmail.com
            // Use locks as suggested to avoid that
            tx.dsl().select(field("pg_advisory_xact_lock(123)")).execute();
            DfsFileTable
                    .createCoordinatorTable(tx.dsl(), coordinatorName, file, IF_NOT_EXISTS | FOREIGN)
                    .execute();
        });

        return super.doCreateFilePartition(file, partitionFile, partition, options);
    }

    @Override
    public DfsFile findFile(String name, int partitions) {
        return new DfsFile(name, partitions, findCandidateFilePartitions(name));
    }

    @Override
    public @Nullable RestoredBackupInfo loadBackupInfo(int jobId, int partition) {
        return null; // TODO:
    }

    @Override
    public void writeBackupInfo(int jobId, int partition, DfsFilePartitionInfo dstFilePartition, @Nullable Integer nextBatchPtr) {
        doWriteBackupInfo(ctx, jobId, partition, dstFilePartition, nextBatchPtr);
    }

    @SuppressFBWarnings("UP_UNUSED_PARAMETER") // TODO: save this
    private void doWriteBackupInfo(DSLContext ctx,
                                   int jobId,
                                   int partition,
                                   @SuppressWarnings("unused")
                                   DfsFilePartitionInfo dstFilePartition, // TODO: save this
                                   @Nullable Integer nextBatchPtr) {
        ctx.insertInto(DATAFLOW_JOBS)
                .columns(DATAFLOW_JOBS.WORKER, DATAFLOW_JOBS.JOBID, DATAFLOW_JOBS.PARTITION, DATAFLOW_JOBS.NEXTBATCHPTR)
                .values(uuid, jobId, partition, nextBatchPtr)
                .onConflictOnConstraint(Keys.UNIQUE_CNSTR_DATAFLOW_JOBS)
                .doUpdate()
                .set(DATAFLOW_JOBS.NEXTBATCHPTR, nextBatchPtr)
                .execute();
    }

    @Override
    public void writeBatchInPartitionAndBackup(int jobId,
                                               int srcPartition,
                                               DfsFilePartitionInfo dstFilePartition,
                                               Collection<Tuple2> tuple,
                                               @Nullable Integer nextBatchPtr) {
        ctx.transaction(tx -> {
            doWriteBatchInPartition(tx.dsl(), dstFilePartition, tuple);
            doWriteBackupInfo(tx.dsl(), jobId, srcPartition, dstFilePartition, nextBatchPtr);
        });
    }

    @Override
    public SequencedCollection<PreviousJob> readWorkerJobs() {
        return ctx.select(DATAFLOW_JOBS.JOBID, DATAFLOW_JOBS.PARTITION)
                .from(DATAFLOW_JOBS)
                .where(DATAFLOW_JOBS.WORKER.eq(uuid))
                .stream()
                .map(r -> new PreviousJob(
                        r.get(DATAFLOW_JOBS.JOBID),
                        r.get(DATAFLOW_JOBS.PARTITION)))
                .toList();
    }

    //TODO: Verify if it's needed
    @Override
    public void deleteBackup(int jobId, int partition) {
        ctx.delete(DATAFLOW_JOBS)
                .where(DATAFLOW_JOBS.WORKER.eq(uuid)
                        .and(DATAFLOW_JOBS.JOBID.eq(jobId))
                        .and(DATAFLOW_JOBS.PARTITION.eq(partition)))
                .execute();
    }
}
