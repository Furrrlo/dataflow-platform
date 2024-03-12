package it.polimi.ds.dataflow.coordinator.dfs;

import com.google.errorprone.annotations.MustBeClosed;
import com.zaxxer.hikari.HikariConfig;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import it.polimi.ds.dataflow.utils.FastIllegalStateException;
import org.jetbrains.annotations.Unmodifiable;
import org.jetbrains.annotations.VisibleForTesting;
import org.jooq.impl.SQLDataType;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.*;
import static org.jooq.impl.DSL.name;

public class PostgresCoordinatorDfs extends PostgresDfs implements CoordinatorDfs {

    private final Set<String> foreignServers = ConcurrentHashMap.newKeySet();

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
    public DfsFile createPartitionedFilePreemptively(String name, int partitions) {
        createCoordinatorTable(ctx, LOCAL_DFS_NODE_NAME, name, 0).execute();

        var foreignServers = List.copyOf(this.foreignServers);
        if (foreignServers.isEmpty())
            throw new IllegalStateException("No active workers connected");

        return new DfsFile(name, IntStream.range(0, partitions)
                .mapToObj(partitionIdx -> {
                    String server = foreignServers.get(partitionIdx % foreignServers.size());
                    var partition = new DfsFilePartitionInfo(
                            name, name + "_" + partitionIdx, partitionIdx, server, false);
                    ctx.execute(STR."""
                             CREATE FOREIGN TABLE \{ctx.render(partitionTableFor(partition))}
                             PARTITION OF \{ctx.render(coordinatorTableFor(name))}
                             FOR VALUES FROM (\{partitionIdx}) TO (\{partitionIdx + 1})
                             SERVER \{server}
                             """);
                    return partition;
                })
                .toList());
    }

    @Override
    @SuppressWarnings("TrailingWhitespacesInTextBlock")
    public @Unmodifiable DfsFile createPartitionedFile(String name, SequencedCollection<DfsFilePartitionInfo> partitions) {
        final Map<Integer, List<DfsFilePartitionInfo>> partitionsToTableNames = partitions.stream()
                .collect(Collectors.groupingBy(
                        DfsFilePartitionInfo::partition,
                        Collectors.toList()));

        final var exs = new ArrayList<RuntimeException>(partitions.stream()
                .filter(p -> !p.fileName().equals(name))
                .map(p -> new FastIllegalStateException("Partition " + p + " does not refer to file " + name))
                .toList());

        final var missingPartitions = new ArrayList<Integer>();
        final int maxPartition = partitionsToTableNames.keySet().stream().mapToInt(i -> i).max().orElse(-1);
        for(int i = 0; i <= maxPartition; i++) {
            if(partitionsToTableNames.get(i) == null)
                missingPartitions.add(i);
        }

        if(!missingPartitions.isEmpty())
            exs.add(new FastIllegalStateException("Missing partitions " + missingPartitions));

        exs.addAll(partitionsToTableNames.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .map(e -> new FastIllegalStateException(
                        "Duplicate partition names for " + e.getKey() + ": " + e.getValue()))
                .toList());

        if(!exs.isEmpty()) {
            var ex = new IllegalStateException("Failed to create partitioned file " + name);
            exs.forEach(ex::addSuppressed);
            throw ex;
        }

        createCoordinatorTable(ctx, LOCAL_DFS_NODE_NAME, name, 0).execute();
        return new DfsFile(name, partitions.stream()
                .map(p -> {
                    ctx.execute(STR."""
                             CREATE FOREIGN TABLE \{ctx.render(partitionTableFor(p))}
                             PARTITION OF \{ctx.render(coordinatorTableFor(name))}
                             FOR VALUES FROM (\{p.partition()}) TO (\{p.partition() + 1})
                             SERVER \{p.dfsNodeName()}
                             """);
                    return new DfsFilePartitionInfo(
                            name, p.partitionFileName(), p.partition(), p.dfsNodeName(), false);
                })
                .toList());
    }

    @Override
    public DfsFile findFile(String name) {
        return new DfsFile(name, findCandidateFilePartitions(name));
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
