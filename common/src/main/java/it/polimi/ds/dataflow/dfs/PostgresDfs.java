package it.polimi.ds.dataflow.dfs;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import it.polimi.ds.dataflow.utils.UncheckedInterruptedException;
import org.intellij.lang.annotations.MagicConstant;
import org.jetbrains.annotations.NotNull;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.SQLDataType;
import org.jspecify.annotations.Nullable;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.random.RandomGenerator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static it.polimi.ds.dataflow.dfs.PostgresDfs.DfsFileTable.*;
import static org.jooq.JSONB.jsonb;
import static org.jooq.impl.DSL.*;

public class PostgresDfs implements Dfs {

    protected static final String LOCAL_DFS_NODE_NAME = "localhost";
    protected static final int POSTGRES_IDENTIFIER_MAX_LEN = 63;

    protected final DataSource dataSource;
    protected final DSLContext ctx;
    protected final Tuple2JsonSerde serde;
    protected final RandomGenerator rnd = RandomGenerator.getDefault();

    protected PostgresDfs(ScriptEngine engine, Consumer<HikariConfig> configurator) throws ScriptException {
        this(new JacksonTuple2Serde(engine), configurator);
    }

    protected PostgresDfs(Tuple2JsonSerde serde, Consumer<HikariConfig> configurator) {
        this.serde = serde;

        var config = new HikariConfig();
        configurator.accept(config);
        this.dataSource = new HikariDataSource(config);

        this.ctx = using(new DefaultConfiguration()
                .set(SQLDialect.POSTGRES)
                .set(dataSource)
                .set(new TranslateInterruptedExceptionExecuteListener()));
    }

    @SuppressWarnings({
            "serial", // Don't care about this being serializable
            "RedundantSuppression" // Javac complains about serial, IntelliJ about the suppression
    })
    private static final class TranslateInterruptedExceptionExecuteListener implements ExecuteListener {

        @Override
        public void exception(ExecuteContext ctx) {
            if(Thread.interrupted()) {
                var ex = ctx.sqlException() != null
                        ? ctx.sqlException()
                        : ctx.exception() != null
                        ? ctx.exception()
                        : null;
                ctx.exception(new UncheckedInterruptedException(ex));
            }
        }
    }

    @Override
    @SuppressFBWarnings(
            value = "BC_VACUOUS_INSTANCEOF",
            justification = "Don't want to have to declare it as a HikariDataSource")
    public void close() throws IOException {
        if (dataSource instanceof Closeable closeableDs)
            closeableDs.close();
    }

    protected DataAccessException translateException(DataAccessException ex) {
        if(Thread.interrupted())
            throw new UncheckedInterruptedException(ex);
        throw ex;
    }

    @Override
    public final DfsFilePartitionInfo createFilePartition(String file, int partition, CreateFileOptions... options) {
        return doCreateFilePartition(file, file + "_" + partition, partition, options);
    }

    @Override
    public DfsFilePartitionInfo createTempFilePartition(String file, int partition, CreateFileOptions... options) {
        final var suffix = "_" + partition;
        final int suffixLen = suffix.length();
        final int remainingLen = POSTGRES_IDENTIFIER_MAX_LEN - file.length() - suffixLen - 1;

        final String partitionFile;
        if(remainingLen == 0) {
            partitionFile = file + suffix;
        } else if(remainingLen < 0) {
            partitionFile = file.substring(0, POSTGRES_IDENTIFIER_MAX_LEN - suffixLen) + suffix;
        } else {
            final String rndStr = rnd.ints('0', 'z' + 1)
                    .filter(i -> (i <= '9' || i >= 'A') && (i <= 'Z' || i >= 'a'))
                    .limit(remainingLen)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            partitionFile = file + "_" + rndStr + suffix;
        }

        return doCreateFilePartition(file, partitionFile, partition, options);
    }

    @Override
    public final DfsFilePartitionInfo createFilePartition(String file,
                                                          String partitionFile,
                                                          int partition,
                                                          CreateFileOptions... options) {
        if (!partitionFile.startsWith(file + "_"))
            throw new IllegalStateException("partitionFileName must start with fileName");
        if (!partitionFile.endsWith("_" + partition))
            throw new IllegalStateException("partitionFileName must end with partition index");
        return doCreateFilePartition(file, partitionFile, partition, options);
    }

    protected DfsFilePartitionInfo doCreateFilePartition(String file,
                                                         String partitionFile,
                                                         int partitionIdx,
                                                         CreateFileOptions... options) {
        boolean failIfExists = Arrays.stream(options).noneMatch(o -> o == CreateFileOptions.IF_NOT_EXISTS);
        if (failIfExists && Arrays.stream(options).anyMatch(o -> o == CreateFileOptions.FAIL_IF_EXISTS))
            throw new IllegalStateException("FAIL_IF_EXISTS and IF_NOT_EXISTS cannot be specified together");

        var partition = new DfsFilePartitionInfo(file, partitionFile, partitionIdx, LOCAL_DFS_NODE_NAME, true);
        createPartitionTable(ctx, partition, failIfExists ? 0 : IF_NOT_EXISTS).execute();
        return partition;
    }

    protected List<DfsFilePartitionInfo> findCandidateFilePartitions(String name) {
        record TmpTableData(String tablename, String srvname, boolean isLocal) {
        }

        String tableRegex = STR."^\{name}(_.*|)_(0|[1-9][0-9]*)$";
        return Stream.concat(
                        // Could use ctx.meta() instead, but it would use a less efficient query
                        ctx.select(PgTables.TABLENAME)
                                .from(PgTables.PG_TABLES)
                                .where(PgTables.SCHEMANAME.notEqual("pg_catalog")
                                        .and(PgTables.SCHEMANAME.notEqual("information_schema"))
                                        .and(PgTables.TABLENAME.likeRegex(tableRegex)))
                                .stream()
                                .map(r -> new TmpTableData(r.get(PgTables.TABLENAME), LOCAL_DFS_NODE_NAME, true)),
                        ctx.select(PgClass.RELNAME, PgForeignServer.SRVNAME)
                                .from(PgForeignTable.PG_FOREIGN_TABLE
                                        .join(PgClass.PG_CLASS)
                                        .on(PgForeignTable.FTRELID.eq(PgClass.OID))
                                        .join(PgForeignServer.PG_FOREIGN_SERVER)
                                        .on(PgForeignTable.FTSERVER.eq(PgForeignServer.OID)))
                                .where(PgClass.RELNAME.likeRegex(tableRegex))
                                .stream()
                                .map(r -> new TmpTableData(
                                        r.get(PgClass.RELNAME),
                                        r.get(PgForeignServer.SRVNAME),
                                        false)))
                .map(r -> {
                    var currRelName = r.tablename();

                    final int partition;
                    try {
                        partition = Integer.parseInt(currRelName.substring(
                                currRelName.lastIndexOf('_') + 1));
                    } catch (NumberFormatException ex) {
                        throw new AssertionError(STR."Unexpectedly failed to parse integer for \{currRelName}", ex);
                    }

                    return new DfsFilePartitionInfo(name, currRelName, partition, r.srvname, r.isLocal);
                })
                .toList();
    }

    @Override
    public void write(DfsFile file, Tuple2 tuple) {
        writeInPartition(file, tuple, calculatePartition(tuple, file.partitionsNum()));
    }

    @Override
    public void writeInPartition(DfsFile file, Tuple2 tuple, int partitionIdx) {
        var maybePartition = file.maybePartitionOf(partitionIdx);
        var table = maybePartition != null && maybePartition.isLocal()
                ? partitionTableFor(maybePartition)
                : coordinatorTableFor(file);
        ctx.insertInto(table, PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                .values(partitionIdx, hash(tuple), jsonb(serde.jsonify(tuple)))
                .execute();
    }

    @Override
    public void writeBatch(DfsFile file, Collection<Tuple2> tuples) {
        doWriteBatch(ctx, file, tuples);
    }

    protected void doWriteBatch(DSLContext ctx, DfsFile file, Collection<Tuple2> tuples) {
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

        localBatches.forEach((partition, data) -> doWriteBatchInPartition(ctx, file, partition, data));
    }

    @Override
    public void writeBatchInPartition(DfsFile file, int partition, Collection<Tuple2> tuples) {
        doWriteBatchInPartition(ctx, file, partition, tuples);
    }

    protected void doWriteBatchInPartition(DSLContext ctx, DfsFile file, int partitionIdx, Collection<Tuple2> tuples) {
        var maybePartition = file.maybePartitionOf(partitionIdx);
        ctx.batch(tuples.stream()
                .map(tuple -> ctx
                        .insertInto(maybePartition != null && maybePartition.isLocal()
                                        ? partitionTableFor(maybePartition)
                                        : coordinatorTableFor(file.name()),
                                PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                        .values(partitionIdx, hash(tuple), jsonb(serde.jsonify(tuple))))
                .collect(Collectors.toList())
        ).execute();
    }

    protected void doWriteBatchInPartition(DSLContext ctx, DfsFilePartitionInfo dstFilePartition, Collection<Tuple2> tuples) {
        ctx.batch(tuples.stream()
                .map(tuple -> ctx
                        .insertInto(dstFilePartition.isLocal()
                                        ? partitionTableFor(dstFilePartition)
                                        : coordinatorTableFor(dstFilePartition.fileName()),
                                PARTITION_COLUMN, KEY_HASH_COLUMN, DATA_COLUMN)
                        .values(dstFilePartition.partition(), hash(tuple), jsonb(serde.jsonify(tuple))))
                .collect(Collectors.toList())
        ).execute();
    }

    @Override
    public BatchRead readNextBatch(DfsFile file, int partitionIdx, int batchHint, @Nullable Integer nextBatchPtr) {
        var maybePartition = file.maybePartitionOf(partitionIdx);
        var selectStep = ctx
                .select(KEY_HASH_COLUMN, DATA_COLUMN)
                .from(maybePartition != null && maybePartition.isLocal()
                        ? partitionTableFor(maybePartition)
                        : coordinatorTableFor(file));
        SelectOrderByStep<Record2<Integer, JSONB>> orderByStep = nextBatchPtr != null
                ? selectStep.where(KEY_HASH_COLUMN.gt(nextBatchPtr))
                : selectStep;

        final var data = orderByStep
                .orderBy(KEY_HASH_COLUMN)
                .limit(batchHint).withTies()
                .fetch();
        final var ptr = !data.isEmpty() ? data.getLast().get(KEY_HASH_COLUMN) : null;
        return new BatchRead(
                ptr,
                data.map(r -> serde.parseJson(r.get(DATA_COLUMN).data())));
    }

    protected int hash(Tuple2 tuple) {
        return tuple.key().hashCode();
    }

    protected int calculatePartition(Tuple2 tuple, int partitions) {
        return (int) (Integer.toUnsignedLong(hash(tuple)) % partitions);
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

        public static Table<Record> partitionTableFor(DfsFilePartitionInfo partition) {
            return table(name(partition.partitionFileName()));
        }

        public static Query createPartitionTable(DSLContext ctx,
                                                 DfsFilePartitionInfo partition,
                                                 @MagicConstant(flags = {IF_NOT_EXISTS}) int flags) {
            boolean ifNotExists = (flags & IF_NOT_EXISTS) != 0;
            var table = partitionTableFor(partition);
            var createTable = ifNotExists
                    ? ctx.createTableIfNotExists(table)
                    : ctx.createTable(table);
            return ctx.begin(
                    createTable
                            .column(PARTITION_COLUMN)
                            .column(KEY_HASH_COLUMN)
                            .column(DATA_COLUMN)
                            .check(PARTITION_COLUMN.eq(partition.partition())),
                    createKeyHashIndex(ctx, table, flags));
        }

        public static Query createCoordinatorTable(DSLContext ctx,
                                                   String coordinatorName,
                                                   String fileName,
                                                   @MagicConstant(flags = {IF_NOT_EXISTS, FOREIGN}) int flags) {
            boolean ifNotExists = (flags & IF_NOT_EXISTS) != 0;
            boolean foreign = (flags & FOREIGN) != 0;

            var table = coordinatorTableFor(fileName);
            var createTable = ifNotExists
                    ? ctx.createTableIfNotExists(table)
                    : ctx.createTable(table);
            var createNonForeignTable = createTable
                    .column(PARTITION_COLUMN)
                    .column(KEY_HASH_COLUMN)
                    .column(DATA_COLUMN);

            if (foreign) {
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
                    createKeyHashIndex(ctx, table, flags));
        }

        @SuppressWarnings("TrailingWhitespacesInTextBlock")
        private static Query createKeyHashIndex(DSLContext ctx,
                                                Table<Record> table,
                                                @MagicConstant(flags = {IF_NOT_EXISTS}) int flags) {
            boolean ifNotExists = (flags & IF_NOT_EXISTS) != 0;
            return ctx.query(STR."""
                     CREATE INDEX \{ifNotExists ? "IF NOT EXISTS " : ""}\
                     \{ctx.render(name(table.getName() + "_keyhash_btree_index"))} \
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
    }

    private static final class PgForeignTable {
        public static final Table<Record> PG_FOREIGN_TABLE =
                table(name("pg_catalog").append("pg_foreign_table"));

        public static final Field<Object> FTSERVER = field(PG_FOREIGN_TABLE.getQualifiedName().append("ftserver"));
        public static final Field<Object> FTRELID = field(PG_FOREIGN_TABLE.getQualifiedName().append("ftrelid"));
    }

    private static final class PgClass {
        public static final Table<Record> PG_CLASS =
                table(name("pg_catalog").append("pg_class"));

        public static final Field<Object> OID = field(PG_CLASS.getQualifiedName().append("oid"));
        public static final Field<String> RELNAME =
                field(PG_CLASS.getQualifiedName().append("relname"), SQLDataType.VARCHAR);
    }

    private static final class PgForeignServer {
        public static final Table<Record> PG_FOREIGN_SERVER = table(name("pg_foreign_server"));

        public static final Field<Object> OID = field(PG_FOREIGN_SERVER.getQualifiedName().append("oid"));
        public static final Field<String> SRVNAME =
                field(PG_FOREIGN_SERVER.getQualifiedName().append("srvname"), SQLDataType.VARCHAR);
    }
    @Override
    public void deleteFile(String name) {
        ctx.dropTable(name).execute();
    }
}
