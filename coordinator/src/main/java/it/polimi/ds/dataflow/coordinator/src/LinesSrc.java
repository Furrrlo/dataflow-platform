package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.dataflow.Tuple2;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Stream;

public final class LinesSrc implements NonPartitionedCoordinatorSrc {

    private final WorkDirFileLoader loader;
    private final String fileName;
    private final int partitions;

    public LinesSrc(WorkDirFileLoader loader, String fileName, int partitions) {
        this.loader = loader;
        this.fileName = fileName;
        this.partitions = partitions;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    @SuppressFBWarnings(
            value = "OS_OPEN_STREAM",
            justification = "The stream is closeable and closes the Reader")
    public Stream<Tuple2> loadAll() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                loader.loadResourceAsStream(fileName),
                StandardCharsets.UTF_8));

        final Stream<Tuple2> stream = br.lines().map(s -> new Tuple2(s, null));
        return stream.onClose(() -> {
            try {
                br.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public int requestedPartitions() {
        return partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LinesSrc linesSrc = (LinesSrc) o;
        return partitions == linesSrc.partitions &&
                Objects.equals(loader, linesSrc.loader) &&
                Objects.equals(fileName, linesSrc.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loader, fileName, partitions);
    }

    @Override
    public String toString() {
        return "LinesSrc{" +
                "loader=" + loader +
                ", fileName='" + fileName + '\'' +
                ", partitions=" + partitions +
                '}';
    }
}
