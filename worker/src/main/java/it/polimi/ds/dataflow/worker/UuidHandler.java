package it.polimi.ds.dataflow.worker;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class UuidHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(UuidHandler.class);
    private static final String DEFAULT_UUID_FILE_NAME = "uuid-storage.txt";

    private UuidHandler() {
    }

    public static UUID getUuid(WorkDirFileLoader fileLoader) throws IOException {
        try {
            return getUuid(fileLoader, DEFAULT_UUID_FILE_NAME);
        } catch (IOException ex) {
            throw new IOException("Failed to get or create UUID", ex);
        }
    }

    @VisibleForTesting
    @SuppressWarnings("SameParameterValue")
    static UUID getUuid(WorkDirFileLoader fileLoader, String uuidFileName) throws IOException {
        final Path uuidFile = fileLoader.resolvePath(uuidFileName);
        if (!Files.exists(uuidFile)) {
            LOGGER.trace("File creation...");
            Files.createFile(uuidFile);
            LOGGER.trace("{} file created", uuidFile.toAbsolutePath());
            return createNewUuid(uuidFile);
        }

        try (BufferedReader reader = Files.newBufferedReader(uuidFile, StandardCharsets.UTF_8)) {
            var uuid = UUID.fromString(reader.readLine());
            LOGGER.trace("UUID read from existing file: {}", uuid);
            return uuid;
        }
    }

    private static UUID createNewUuid(Path uuidFile) throws IOException {
        UUID uuid = UUID.randomUUID();
        try (BufferedWriter fileWriter = Files.newBufferedWriter(uuidFile, StandardCharsets.UTF_8)) {
            fileWriter.write(uuid.toString());
            fileWriter.newLine();
            LOGGER.trace("UUID generated");
        }
        return uuid;
    }
}
