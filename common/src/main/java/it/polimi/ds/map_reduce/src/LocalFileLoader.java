package it.polimi.ds.map_reduce.src;

import it.polimi.ds.map_reduce.utils.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

@SuppressFBWarnings(
        value = "PATH_TRAVERSAL_IN",
        justification = "All path are manually checked for traversal issues")
public class LocalFileLoader {

    private final Path baseDir;

    public LocalFileLoader(Path baseDir) {
        this.baseDir = baseDir;
    }

    public InputStream loadAsStream(String fileName) throws IOException {
        final InputStream inJarIs = LocalFileLoader.class.getResourceAsStream(fileName);
        if (inJarIs != null)
            return inJarIs;

        return Files.newInputStream(ensureNoFileTraversal(fileName));
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean exists(String file) {
        final URL inJarIs = LocalFileLoader.class.getResource(file);
        return inJarIs != null || Files.exists(ensureNoFileTraversal(file));
    }

    public void createNewFile(String fileName) throws IOException {
        final Path path = ensureNoFileTraversal(fileName);
        Files.createFile(path);
    }

    private Path ensureNoFileTraversal(String fileName) {
        Path path = baseDir.resolve(fileName).normalize();
        if(!path.startsWith(baseDir))
            throw new IllegalStateException(STR."Attempted file traversal \{path}");
        return path;
    }
}
