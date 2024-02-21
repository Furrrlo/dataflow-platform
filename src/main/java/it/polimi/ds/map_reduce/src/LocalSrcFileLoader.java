package it.polimi.ds.map_reduce.src;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalSrcFileLoader {

    private final Path baseDir;

    public LocalSrcFileLoader(Path baseDir) {
        this.baseDir = baseDir;
    }

    public InputStream loadAsStream(String fileName) throws IOException {
        final InputStream inJarIs = LocalSrcFileLoader.class.getResourceAsStream(fileName);
        if(inJarIs != null)
            return inJarIs;

        return Files.newInputStream(baseDir.resolve(fileName));
    }

    public boolean exists(String file) {
        final URL inJarIs = LocalSrcFileLoader.class.getResource(file);
        if(inJarIs != null)
            return true;

        return Files.exists(baseDir.resolve(file));
    }
}
