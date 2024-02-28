package it.polimi.ds.dataflow.dfs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jspecify.annotations.Nullable;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.*;
import static org.jooq.JSONB.jsonb;
import static org.jooq.impl.DSL.*;

public class PostgresDfs implements Dfs {

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
    public void createFilePartition(String file, int partition, CreateFileOptions... options) {
        boolean failIfExists = Arrays.stream(options).noneMatch(o -> o == CreateFileOptions.IF_NOT_EXISTS);
        if(failIfExists && Arrays.stream(options).anyMatch(o -> o == CreateFileOptions.FAIL_IF_EXISTS))
            throw new IllegalStateException("FAIL_IF_EXISTS and IF_NOT_EXISTS cannot be specified together");

        boolean hasCoordinator = coordinatorName != null;
        if(hasCoordinator) {
            DfsFileTable
                    .createCoordinatorTable(ctx, coordinatorName, file, IF_NOT_EXISTS | FOREIGN)
                    .execute();
        }

        DfsFileTable
                .createPartitionTable(ctx, file, partition, failIfExists ? 0 : IF_NOT_EXISTS)
                .execute();
    }

    @Override
    public DfsFile findFile(String name) {
        record TmpTableData(String tablename, String srvname, boolean isLocal) {
        }

        return new DfsFile(name, Stream.concat(
                        // Could use ctx.meta() instead, but it would use a less efficient query
                        ctx.select(PgTables.TABLENAME)
                                .from(PgTables.PG_TABLES)
                                .where(PgTables.SCHEMANAME.notEqual("pg_catalog")
                                        .and(PgTables.SCHEMANAME.notEqual("information_schema"))
                                        .and(PgTables.TABLENAME.like(STR."\{name}_%")))
                                .stream()
                                .map(r -> new TmpTableData(r.get(PgTables.TABLENAME), "localhost", true)),
                        ctx.select(PgClass.RELNAME, PgForeignServer.SRVNAME)
                                .from(PgForeignTable.PG_FOREIGN_TABLE
                                        .join(PgClass.PG_CLASS)
                                        .on(PgForeignTable.FTRELID.eq(PgClass.OID))
                                        .join(PgForeignServer.PG_FOREIGN_SERVER)
                                        .on(PgForeignTable.FTSERVER.eq(PgForeignServer.OID)))
                                .where(PgClass.RELNAME.like(STR."\{name}_%"))
                                .stream()
                                .map(r -> new TmpTableData(
                                        r.get(PgClass.RELNAME),
                                        r.get(PgForeignServer.SRVNAME),
                                        false)))
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
        writeInPartition(file, tuple, calculatePartition(tuple, file.partitionsNum()));
    }

    @Override
    public void writeInPartition(DfsFile file, Tuple2 tuple, int partition) {
        boolean isLocal = isPartitionLocal(file, partition);

        var table = isLocal
                ? partitionTableFor(file, partition)
                : coordinatorTableFor(file);
        ctx.insertInto(table, PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                .values(partition, hash(tuple), jsonb(serde.jsonify(tuple)))
                .execute();
    }

    @Override
    public void writeBatch(DfsFile file, Collection<Tuple2> tuples) {
        Map<Integer, List<Tuple2>> batches = tuples.stream().collect(Collectors.groupingBy(
                e -> calculatePartition(e, file.partitionsNum()),
                Collectors.toList()));

        Map<Integer, List<Tuple2>> localBatches = file.partitions().stream()
                .filter(DfsFilePartitionInfo::isLocal)
                .filter(p -> batches.containsKey(p.partition()))
                .collect(Collectors.toMap(
                        DfsFilePartitionInfo::partition,
                        p -> batches.getOrDefault(p.partition(), List.of())));
        Map<Integer, List<Tuple2>> nonLocalBatches = batches.keySet().stream()
                .filter(p -> !localBatches.containsKey(p))
                .collect(Collectors.toMap(
                        Function.identity(),
                        p -> batches.getOrDefault(p, List.of())));

        ctx.batch(
                nonLocalBatches.entrySet().stream().flatMap(e -> {
                    int partition = e.getKey();
                    var data = e.getValue();

                    return data.stream().map(tuple -> ctx
                            .insertInto(coordinatorTableFor(file), PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                            .values(partition, hash(tuple), jsonb(serde.jsonify(tuple))));
                }).collect(Collectors.toList())
        ).execute();

        localBatches.forEach((partition, data) -> writeBatchInPartition(file, partition, data));
    }

    @Override
    public void writeBatchInPartition(DfsFile file, int partition, Collection<Tuple2> tuples) {
        boolean isLocal = isPartitionLocal(file, partition);

        ctx.batch(tuples.stream()
                .map(tuple -> ctx
                        .insertInto(isLocal
                                        ? partitionTableFor(file, partition)
                                        : coordinatorTableFor(file.name()),
                                PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                        .values(partition, hash(tuple), jsonb(serde.jsonify(tuple))))
                .collect(Collectors.toList())
        ).execute();
    }

    @Override
    public BatchRead readNextBatch(DfsFile file, int partition, int batchHint, @Nullable Integer nextBatchPtr) {
        var selectStep = ctx
                .select(KEY_HASH_COLUMN, DATA_COLUMN)
                .from(isPartitionLocal(file, partition)
                        ? partitionTableFor(file, partition)
                        : coordinatorTableFor(file));
        SelectOrderByStep<Record2<Integer, JSONB>> orderByStep = nextBatchPtr != null
                ? selectStep.where(KEY_HASH_COLUMN.gt(nextBatchPtr))
                : selectStep;

        final var data = orderByStep
                .orderBy(KEY_HASH_COLUMN)
                .limit(batchHint).withTies()
                .fetch();
        final var ptr = data.getLast().get(KEY_HASH_COLUMN);
        return new BatchRead(
                ptr,
                data.map(r -> serde.parseJson(r.get(DATA_COLUMN).data())));
    }

    protected int hash(Tuple2 tuple) {
        return tuple.key().hashCode();
    }

    protected int calculatePartition(Tuple2 tuple, int partitions) {
        return hash(tuple) % partitions;
    }

    protected boolean isPartitionLocal(DfsFile file, int partition) {
        return file.partitions().stream()
                .filter(p -> p.partition() == partition)
                .findFirst()
                .map(DfsFilePartitionInfo::isLocal)
                .orElse(Boolean.FALSE);
    }

    public static final class DfsFileTable {

        public static final Field<Integer> PARTITION_COLUMN = field(
                name("partition"),
                SQLDataType.INTEGER.notNull());
        public static final Field<Integer> KEY_HASH_COLUMN = field(
                name("keyhash"),
                SQLDataType.INTEGER.notNull());
        public static final Field<JSONB> DATA_COLUMN = field(
                name("data"),
                SQLDataType.JSONB.notNull());

        public static final int IF_NOT_EXISTS = 0x1;
        public static final int FOREIGN = 0x1;

        public static @NotNull Table<Record> coordinatorTableFor(String fileName) {
            return table(name(fileName));
        }

        public static Table<Record> coordinatorTableFor(DfsFile file) {
            return coordinatorTableFor(file.name());
        }

        public static Table<Record> partitionTableFor(String fileName, int partition) {
            return table(name(STR."\{fileName}_\{partition}"));
        }

        public static Table<Record> partitionTableFor(DfsFile file, int partition) {
            return partitionTableFor(file.name(), partition);
        }

        public static Query createPartitionTable(DSLContext ctx,
                                                 String fileName,
                                                 int partition,
                                                 @MagicConstant(flags = { IF_NOT_EXISTS }) int flags) {
            boolean ifNotExists = (flags & IF_NOT_EXISTS) != 0;
            var table = DfsFileTable.partitionTableFor(fileName, partition);
            var createTable = ifNotExists
                    ? ctx.createTableIfNotExists(table)
                    : ctx.createTable(table);
            return ctx.begin(
                    createTable
                            .column(PARTITION_COLUMN)
                            .column(KEY_HASH_COLUMN)
                            .column(DATA_COLUMN)
                            .check(PARTITION_COLUMN.eq(partition)),
                    createKeyHashIndex(ctx, table));
        }

        public static Query createCoordinatorTable(DSLContext ctx,
                                                   String coordinatorName,
                                                   String fileName,
                                                   @MagicConstant(flags = { IF_NOT_EXISTS, FOREIGN }) int flags) {
            boolean ifNotExists = (flags & IF_NOT_EXISTS) != 0;
            boolean foreign = (flags & FOREIGN) != 0;

            var table = DfsFileTable.coordinatorTableFor(fileName);
            var createTable = ifNotExists
                    ? ctx.createTableIfNotExists(table)
                    : ctx.createTable(table);
            var createNonForeignTable = createTable
                    .column(PARTITION_COLUMN)
                    .column(KEY_HASH_COLUMN)
                    .column(DATA_COLUMN);

            if(foreign) {
                // Cannot declare indexes on foreign tables
                return ctx.query(STR."""
                        \{
                        createNonForeignTable.getSQL().replaceFirst(
                                STR."(?i)\{Pattern.quote("CREATE TABLE")}",
                                "CREATE FOREIGN TABLE")
                        } SERVER \{coordinatorName}
                        """);
            }

            return ctx.begin(
                    ctx.query(STR."""
                        \{createNonForeignTable.getSQL()} \
                        PARTITION BY RANGE (partition)\
                        """),
                    createKeyHashIndex(ctx, table));
        }

        @SuppressWarnings("TrailingWhitespacesInTextBlock")
        private static Query createKeyHashIndex(DSLContext ctx, Table<Record> table) {
            return ctx.query(STR."""
                     CREATE INDEX \{ctx.render(name(table.getName() + "_keyhash_btree_index"))} \
                     ON \{ctx.render(table)} \
                     USING btree (\{ctx.render(KEY_HASH_COLUMN)} ASC)\
                     """);
        }

        private DfsFileTable() {
        }
    }

    private static final class PgTables {
        public static final Table<Record> PG_TABLES = table(name("pg_catalog").append("pg_tables"));

        public static final Field<String> TABLENAME = field(
                PG_TABLES.getQualifiedName().append("tablename"), SQLDataType.VARCHAR);
        public static final Field<Object> SCHEMANAME = field(PG_TABLES.getQualifiedName().append("schemaname"));

        private PgTables() {
        }
    }

    private static final class PgForeignTable {
        public static final Table<Record> PG_FOREIGN_TABLE =
                table(name("pg_catalog").append("pg_foreign_table"));

        public static final Field<Object> FTSERVER = field(PG_FOREIGN_TABLE.getQualifiedName().append("ftserver"));
        public static final Field<Object> FTRELID = field(PG_FOREIGN_TABLE.getQualifiedName().append("ftrelid"));

        private PgForeignTable() {
        }
    }

    private static final class PgClass {
        public static final Table<Record> PG_CLASS =
                table(name("pg_catalog").append("pg_class"));

        public static final Field<Object> OID = field(PG_CLASS.getQualifiedName().append("oid"));
        public static final Field<String> RELNAME =
                field(PG_CLASS.getQualifiedName().append("relname"), SQLDataType.VARCHAR);

        private PgClass() {
        }
    }

    private static final class PgForeignServer {
        public static final Table<Record> PG_FOREIGN_SERVER = table(name("pg_foreign_server"));

        public static final Field<Object> OID = field(PG_FOREIGN_SERVER.getQualifiedName().append("oid"));
        public static final Field<String> SRVNAME =
                field(PG_FOREIGN_SERVER.getQualifiedName().append("srvname"), SQLDataType.VARCHAR);

        private PgForeignServer() {
        }
    }
}
