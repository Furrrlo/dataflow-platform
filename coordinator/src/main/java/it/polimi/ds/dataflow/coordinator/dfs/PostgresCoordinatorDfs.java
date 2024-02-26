package it.polimi.ds.dataflow.coordinator.dfs;

import com.zaxxer.hikari.HikariConfig;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.dfs.DfsFile;
import it.polimi.ds.dataflow.dfs.DfsFilePartitionInfo;
import it.polimi.ds.dataflow.dfs.PostgresDfs;
import it.polimi.ds.dataflow.dfs.Tuple2JsonSerde;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.jooq.JSONB.jsonb;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

public class PostgresCoordinatorDfs extends PostgresDfs implements CoordinatorDfs {

    private final List<String> foreignServers = new ArrayList<>();

    public PostgresCoordinatorDfs(Tuple2JsonSerde serde,
                                  Consumer<HikariConfig> configurator) {
        super(serde, configurator);
    }

    public PostgresCoordinatorDfs(Consumer<HikariConfig> configurator) {
        super(configurator);
    }

    @VisibleForTesting
    void addForeignServer(String foreignServer) {
        foreignServers.add(foreignServer);
    }

    @Override
    @SuppressWarnings({"TrailingWhitespacesInTextBlock"})
    public DfsFile createPartitionedFile(String name, int partitions) {
        ctx.execute(STR."""
                \{
                ctx.createTable(table(name(name)))
                        .column(PARTITION_COLUMN)
                        .column(DATA_COLUMN)
                        .getSQL()
                } PARTITION BY RANGE (partition)
                """);

        return new DfsFile(name, IntStream.range(0, partitions)
                .mapToObj(partition -> {
                    String server = foreignServers.get(partition % foreignServers.size());
                    ctx.execute(STR."""
                             CREATE FOREIGN TABLE \{ctx.render(table(name(STR."\{name}_\{partition}")))}
                             PARTITION OF \{ctx.render(table(name(name)))}
                             FOR VALUES FROM (\{partition}) TO (\{partition + 1})
                             SERVER \{server}
                             """);
                    return new DfsFilePartitionInfo(name, partition, server, false);
                })
                .toList());
    }

    @Override
    public void write(DfsFile file, Stream<Tuple2> tuples) {
        ctx.batch(tuples.map(t -> ctx
                                .insertInto(table(file.name()))
                                .columns(PARTITION_COLUMN, DATA_COLUMN)
                                .values(
                                        calculatePartition(t, file.partitionsNum()),
                                        jsonb(serde.jsonify(t))
                                ))
                        .toList())
                .execute();
    }

    @Override
    public Stream<Tuple2> loadAll(DfsFile file) {
        return ctx.select(DATA_COLUMN)
                .from(table(name(file.name())))
                .stream()
                .map(r -> serde.parseJson(r.get(DATA_COLUMN).data()));
    }
}
