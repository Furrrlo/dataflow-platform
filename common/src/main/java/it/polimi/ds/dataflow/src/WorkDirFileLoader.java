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

    /**
     *
     * @param baseDir base path from which start the file's path resolve
     */
    public WorkDirFileLoader(Path baseDir) {
        this.baseDir = baseDir.toAbsolutePath().normalize();
    }


    /**
     * @param fileName name of the file we want
     * @return control if the file is in the Jar, if true return the file otherwise create a new file
     */
    public InputStream loadResourceAsStream(String fileName) throws IOException {
        final InputStream inJarIs = WorkDirFileLoader.class.getResourceAsStream(fileName);
        if (inJarIs != null)
            return inJarIs;

        return Files.newInputStream(ensureNoFileTraversal(fileName));
    }

    /**
     * @param file name of the file we want
     * @return true if the file exists, otherwise false
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean resourceExists(String file) {
        final URL inJarIs = WorkDirFileLoader.class.getResource(file);
        return inJarIs != null || Files.exists(ensureNoFileTraversal(file));
    }

    /**
     * @param path path of the file we want
     * @return the resolved path, relative to the working directory
     */
    public Path resolvePath(String path) {
        return ensureNoFileTraversal(path);
    }

    /**
     * @param fileName path of the file we want
     * @return the path resolved if is in the workspace
     */
    private Path ensureNoFileTraversal(String fileName) {
        Path path = baseDir.resolve(fileName).normalize();
        if(!path.startsWith(baseDir))
            throw new IllegalStateException(STR."Attempted file traversal \{path.toAbsolutePath()}");
        return path;
    }
}
