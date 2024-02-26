package it.polimi.ds.dataflow.dfs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.jooq.JSONB.jsonb;
import static org.jooq.impl.DSL.*;

public class PostgresDfs implements Dfs {

    protected final Field<Integer> PARTITION_COLUMN = field(name("partition"), Integer.class);
    protected final Field<JSONB> DATA_COLUMN = field(name("data"), JSONB.class);

    protected final DataSource dataSource;
    protected final DSLContext ctx;
    protected final Tuple2JsonSerde serde;

    private final @Nullable String coordinatorName;

    public PostgresDfs(String coordinatorName, Consumer<HikariConfig> configurator) {
        this(coordinatorName, new JacksonTuple2Serde(), configurator);
    }

    public PostgresDfs(String coordinatorName,
                       Tuple2JsonSerde serde,
                       Consumer<HikariConfig> configurator) {
        this(coordinatorName, serde, configurator, null);
    }

    protected PostgresDfs(Consumer<HikariConfig> configurator) {
        this(new JacksonTuple2Serde(), configurator);
    }

    protected PostgresDfs(Tuple2JsonSerde serde,
                          Consumer<HikariConfig> configurator) {
        this(null, serde, configurator, null);
    }

    private PostgresDfs(@Nullable String coordinatorName,
                        Tuple2JsonSerde serde,
                        Consumer<HikariConfig> configurator,
                        @SuppressWarnings("unused") @Nullable Void unused) {
        this.coordinatorName = coordinatorName;
        this.serde = serde;

        var config = new HikariConfig();
        configurator.accept(config);
        this.dataSource = new HikariDataSource(config);

        this.ctx = DSL.using(dataSource, SQLDialect.POSTGRES);
    }

    @Override
    @SuppressFBWarnings(
            value = "BC_VACUOUS_INSTANCEOF",
            justification = "Don't want to have to declare it as a HikariDataSource")
    public void close() throws IOException {
        if(dataSource instanceof Closeable closeableDs)
            closeableDs.close();
    }

    @Override
    public void createFilePartition(String file, int partition) {
        boolean hasCoordinator = coordinatorName != null;
        if(hasCoordinator) {
            var createNonForeignTable = ctx.createTableIfNotExists(table(name(file)))
                    .column(PARTITION_COLUMN)
                    .column(DATA_COLUMN)
                    .getSQL();
            ctx.execute(STR."""
                \{
                    createNonForeignTable.replaceFirst(
                            STR."(?i)\{Pattern.quote("CREATE TABLE")}",
                            "CREATE FOREIGN TABLE")
                    } SERVER \{coordinatorName}
                """);
        }

        ctx.createTable(table(name(STR."\{file}_\{partition}")))
                .column(PARTITION_COLUMN)
                .column(DATA_COLUMN)
                .check(PARTITION_COLUMN.eq(partition))
                .execute();
    }

    @Override
    public DfsFile findFile(String name) {
        Table<Record> pg_tables = table(name("pg_catalog").append("pg_tables"));

        final var tablename = field(pg_tables.getQualifiedName().append("tablename"), SQLDataType.VARCHAR);
        final var schemaname = field(pg_tables.getQualifiedName().append("schemaname"));

        Table<Record> pg_foreign_table = table(name("pg_catalog").append("pg_foreign_table"));
        Table<Record> pg_class = table(name("pg_catalog").append("pg_class")).as("cls");
        Table<Record> pg_foreign_server = table("pg_foreign_server").as("srv");

        final var relname = field(pg_class.getQualifiedName().append("relname"), SQLDataType.VARCHAR);
        final var srvname = field(pg_foreign_server.getQualifiedName().append("srvname"), SQLDataType.VARCHAR);

        record TmpTableData(String tablename, String srvname, boolean isLocal) {
        }

        return new DfsFile(name, Stream.concat(
                        ctx.select(tablename)
                                .from(pg_tables)
                                .where(schemaname.notEqual("pg_catalog")
                                        .and(schemaname.notEqual("information_schema"))
                                        .and(tablename.like(STR."\{name}_%")))
                                .stream()
                                .map(r -> new TmpTableData(r.get(tablename), "localhost", true)),
                        ctx.select(relname, srvname)
                                .from(pg_foreign_table
                                        .join(pg_class).on(field(pg_foreign_table.getQualifiedName().append("ftrelid"))
                                                .eq(field(pg_class.getQualifiedName().append(name("oid")))))
                                        .join(pg_foreign_server).on(field(pg_foreign_table.getQualifiedName().append("ftserver"))
                                                .eq(field(pg_foreign_server.getQualifiedName().append("oid")))))
                                .where(relname.like(STR."\{name}_%"))
                                .stream()
                                .map(r -> new TmpTableData(r.get(relname), r.get(srvname), false)))
                .filter(r -> r.tablename()
                        .substring(name.length() + "_".length())
                        .matches("0|[1-9][0-9]*"))
                .map(r -> {
                    var currRelName = r.tablename();

                    final int partition;
                    try {
                        partition = Integer.parseInt(currRelName
                                .substring(name.length() + "_".length()));
                    } catch (NumberFormatException ex) {
                        throw new AssertionError(STR."Unexpectedly failed to parse integer for \{currRelName}");
                    }

                    return new DfsFilePartitionInfo(name, partition, r.srvname, r.isLocal);
                })
                .toList());
    }

    @Override
    public void write(DfsFile file, Tuple2 tuple) {
        writeInPartition(file, tuple, file.partitionsNum());
    }

    @Override
    public void writeInPartition(DfsFile file, Tuple2 tuple, int partition) {
        boolean isLocal = file.partitions().stream()
                .filter(p -> p.partition() == partition)
                .findFirst()
                .map(DfsFilePartitionInfo::isLocal)
                .orElse(Boolean.FALSE);

        var table = isLocal
                ? table(name(STR."\{file.name()}_\{partition}"))
                : table(name(file.name()));
        ctx.insertInto(table, PARTITION_COLUMN, DATA_COLUMN)
                .values(partition, jsonb(serde.jsonify(tuple)))
                .execute();
    }

    protected int calculatePartition(Tuple2 tuple, int partitions) {
        return tuple.key().hashCode() % partitions;
    }
}
