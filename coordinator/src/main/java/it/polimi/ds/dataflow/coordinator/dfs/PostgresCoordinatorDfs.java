package it.polimi.ds.dataflow.coordinator.dfs;

import com.google.errorprone.annotations.MustBeClosed;
import com.zaxxer.hikari.HikariConfig;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import org.jetbrains.annotations.VisibleForTesting;
import org.jooq.impl.SQLDataType;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.*;
import static org.jooq.impl.DSL.name;

public class PostgresCoordinatorDfs extends PostgresDfs implements CoordinatorDfs {

    private final List<String> foreignServers = new ArrayList<>();

    public PostgresCoordinatorDfs(Tuple2JsonSerde serde,
                                  Consumer<HikariConfig> configurator) {
        super(serde, configurator);
    }

    public PostgresCoordinatorDfs(ScriptEngine engine, Consumer<HikariConfig> configurator) throws ScriptException {
        super(engine, configurator);
    }

    @VisibleForTesting // TODO: make this not public, but package private
    public void addForeignServer(String foreignServer) {
        foreignServers.add(foreignServer);
    }

    @Override
    @SuppressWarnings({"TrailingWhitespacesInTextBlock"})
    public DfsFile createPartitionedFile(String name, int partitions) {
        createCoordinatorTable(ctx, "", name, 0).execute();
        return new DfsFile(name, IntStream.range(0, partitions)
                .mapToObj(partition -> {
                    String server = foreignServers.get(partition % foreignServers.size());
                    ctx.execute(STR."""
                             CREATE FOREIGN TABLE \{ctx.render(partitionTableFor(name, partition))}
                             PARTITION OF \{ctx.render(coordinatorTableFor(name))}
                             FOR VALUES FROM (\{partition}) TO (\{partition + 1})
                             SERVER \{server}
                             """);
                    return new DfsFilePartitionInfo(name, partition, server, false);
                })
                .toList());
    }


    @Override
    public @MustBeClosed Stream<Tuple2> loadAll(DfsFile file) {
        return ctx.select(DATA_COLUMN)
                .from(coordinatorTableFor(file))
                .stream()
                .map(r -> serde.parseJson(r.get(DATA_COLUMN).data()));
    }

    @Override
    public void reshuffle(DfsFile file) {
        var table = coordinatorTableFor(file);
        var newPartitionSql = KEY_HASH_COLUMN
                .cast(SQLDataType.BIGINT)
                .bitAnd(0xffffffffL)
                .mod(file.partitionsNum())
                .cast(SQLDataType.INTEGER);
        // Cannot use an UPDATE statement which changes the partition key directly,
        // so remove and add back in a transaction
        ctx.transaction(tx -> {
            var withTable = name("shuffled")
                    .as(tx.dsl().deleteFrom(table)
                            .where(PARTITION_COLUMN.notEqual(newPartitionSql))
                            .returningResult(newPartitionSql.as("partition"), KEY_HASH_COLUMN, DATA_COLUMN));
            tx.dsl().with(withTable)
                    .insertInto(table, PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                    .select(tx.dsl().selectFrom(withTable))
                    .execute();
        });
    }
}
