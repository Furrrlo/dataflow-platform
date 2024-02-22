package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.Tuple2;
import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public final class LinesSrc implements Src {

    private final String fileName;

    public LinesSrc(String fileName) {
        this.fileName = fileName;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    @SuppressFBWarnings(
            value = "OS_OPEN_STREAM",
            justification = "The stream is closeable and closes the Reader")
    public Stream<Tuple2> loadInitial(LocalSrcFileLoader loader) throws IOException {
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
