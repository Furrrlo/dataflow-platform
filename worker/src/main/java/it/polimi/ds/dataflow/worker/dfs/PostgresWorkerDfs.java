package it.polimi.ds.dataflow.worker.dfs;

import com.google.errorprone.annotations.MustBeClosed;
import com.zaxxer.hikari.HikariConfig;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import org.jooq.CreateTableElementListStep;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.*;

public class PostgresWorkerDfs extends PostgresDfs implements WorkerDfs {
    public PostgresWorkerDfs(ScriptEngine engine, String coordinatorName, Consumer<HikariConfig> configurator) throws ScriptException {
        super(engine, coordinatorName, configurator);
    }

    public PostgresWorkerDfs(String coordinatorName, Tuple2JsonSerde serde, Consumer<HikariConfig> configurator) {
        super(coordinatorName, serde, configurator);
    }

    @Override
    public void createBackupFile(String name) {
        var table = coordinatorTableFor(name);
        CreateTableElementListStep createTable;

        //IfNotExists used because the table could already exist if the worker crashed
        createTable = ctx.createTableIfNotExists(table);

        var createTableWithColumns = createTable
                .column(UUID_COLUMN)
                .column(JOB_ID_COLUMN)
                .column(PARTITION_COLUMN)
                .column(NEXT_BATCH_PTR_COLUMN);

        createTableWithColumns.execute();
    }

    @Override
    public void updateBackupFile(String file, BackupInfo backupInfo) {
        ctx.update(coordinatorTableFor(file))
                .set(JOB_ID_COLUMN, backupInfo.jobId())
                .set(PARTITION_COLUMN, backupInfo.partition())
                .set(NEXT_BATCH_PTR_COLUMN, backupInfo.nextBatchPtr())
                .where(UUID_COLUMN.eq(backupInfo.uuid()))
                .execute();
    }

    @Override
    public void writeBackupInfo(String file, BackupInfo backupInfo) {
        ctx.insertInto(coordinatorTableFor(file),
                        UUID_COLUMN,
                        JOB_ID_COLUMN,
                        PARTITION_COLUMN,
                        NEXT_BATCH_PTR_COLUMN)
                .values(backupInfo.uuid(),
                        backupInfo.jobId(),
                        backupInfo.partition(),
                        backupInfo.nextBatchPtr())
                .execute();
    }

    //TODO: Verify if it's needed
    @Override
    public void deleteBackupFile(String file, UUID toDelete) {
        ctx.delete(coordinatorTableFor(file))
                .where(UUID_COLUMN.eq(toDelete))
                .execute();
    }

    @Override
    public @MustBeClosed Stream<BackupInfo> loadAll(String name) {
        return ctx.select(UUID_COLUMN, JOB_ID_COLUMN, PARTITION_COLUMN, NEXT_BATCH_PTR_COLUMN)
                .from(coordinatorTableFor(name))
                .stream()
                .map(r -> new BackupInfo(r.get(UUID_COLUMN),
                        r.get(JOB_ID_COLUMN),
                        r.get(PARTITION_COLUMN),
                        r.get(NEXT_BATCH_PTR_COLUMN)));

    }

    @Override
    public String findFile(String name) {
        var file = super.findFile(name, 0);
        return file.name();
    }
}
