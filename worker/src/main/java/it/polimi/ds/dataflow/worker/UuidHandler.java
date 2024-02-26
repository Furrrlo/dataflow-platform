package it.polimi.ds.dataflow.worker;

import it.polimi.ds.map_reduce.src.LocalFileLoader;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class UuidHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UuidHandler.class);
    private static final String DEFAULT_UUID_FILE_NAME = "uuid-storage.txt";

    private UuidHandler() {
    }

    public static UUID getUuid(LocalFileLoader fileLoader) throws IOException {
        try {
            return getUuid(fileLoader, DEFAULT_UUID_FILE_NAME);
        } catch (IOException ex) {
            throw new IOException("Failed to get or create UUID", ex);
        }
    }

    @VisibleForTesting
    @SuppressWarnings("SameParameterValue")
    static UUID getUuid(LocalFileLoader fileLoader, String uuidFileName) throws IOException {
        if (!fileLoader.exists(uuidFileName)) {
            LOGGER.trace("File creation...");
            fileLoader.createNewFile(uuidFileName);
            LOGGER.trace("{} file created", uuidFileName);
            return createNewUuid();
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileLoader.loadAsStream(DEFAULT_UUID_FILE_NAME), StandardCharsets.UTF_8))) {
            var uuid = UUID.fromString(reader.readLine());
            LOGGER.trace("UUID read from existing file: {}", uuid);
            return uuid;
        }
    }

    private static UUID createNewUuid() throws IOException {
        UUID uuid = UUID.randomUUID();
        try (BufferedWriter fileWriter = Files.newBufferedWriter(
                Path.of(DEFAULT_UUID_FILE_NAME),
                StandardCharsets.UTF_8)) {

            fileWriter.write(uuid.toString());
            fileWriter.newLine();
            LOGGER.trace("UUID generated");
        }
        return uuid;
    }
}
