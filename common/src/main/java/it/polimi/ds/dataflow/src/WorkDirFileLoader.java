package it.polimi.ds.dataflow.src;

import it.polimi.ds.dataflow.utils.SuppressFBWarnings;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

@SuppressFBWarnings(
        value = "PATH_TRAVERSAL_IN",
        justification = "All path are manually checked for traversal issues")
public class WorkDirFileLoader {

    private final Path baseDir;

    public WorkDirFileLoader(Path baseDir) {
        this.baseDir = baseDir.toAbsolutePath().normalize();
    }

    public InputStream loadResourceAsStream(String fileName) throws IOException {
        final InputStream inJarIs = WorkDirFileLoader.class.getResourceAsStream(fileName);
        if (inJarIs != null)
            return inJarIs;

        return Files.newInputStream(ensureNoFileTraversal(fileName));
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean resourceExists(String file) {
        final URL inJarIs = WorkDirFileLoader.class.getResource(file);
        return inJarIs != null || Files.exists(ensureNoFileTraversal(file));
    }

    public Path resolvePath(String path) {
        return ensureNoFileTraversal(path);
    }

    private Path ensureNoFileTraversal(String fileName) {
        Path path = baseDir.resolve(fileName).normalize();
        if(!path.startsWith(baseDir))
            throw new IllegalStateException(STR."Attempted file traversal \{path.toAbsolutePath()}");
        return path;
    }
}
