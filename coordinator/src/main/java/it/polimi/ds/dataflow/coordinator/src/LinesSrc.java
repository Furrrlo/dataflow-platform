package it.polimi.ds.dataflow.coordinator.src;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public final class LinesSrc implements CoordinatorSrc {

    private final LocalSrcFileLoader loader;
    private final String fileName;

    public LinesSrc(LocalSrcFileLoader loader, String fileName) {
        this.loader = loader;
        this.fileName = fileName;
    }

    @Override
    public boolean isNonPartitioned() {
        return true;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    @SuppressFBWarnings(
            value = "OS_OPEN_STREAM",
            justification = "The stream is closeable and closes the Reader")
    public Stream<Tuple2> loadAll() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(
                loader.loadAsStream(fileName),
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
}
