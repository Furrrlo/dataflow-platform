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
        if (inJarIs != null)
            return inJarIs;

        return Files.newInputStream(baseDir.resolve(fileName));
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean exists(String file) {
        final URL inJarIs = LocalSrcFileLoader.class.getResource(file);
        return inJarIs != null || Files.exists(baseDir.resolve(file));
    }

    public void createNewFile(String fileName) {
        File file = new File(baseDir + fileName);
        try {
            if (file.createNewFile())
                System.out.println("New file created correctly");
            else
                System.out.println("The file already exists, skipping creation");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
