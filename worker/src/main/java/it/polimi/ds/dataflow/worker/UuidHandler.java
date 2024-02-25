package it.polimi.ds.dataflow.worker;

import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

public class UuidHandler {
    private static final String uuidFileName = "uuid-storage.txt";

    private static final Logger LOGGER = LoggerFactory.getLogger(UuidHandler.class);

    public UuidHandler() {
    }

    public static UUID getUUID() {
        final LocalSrcFileLoader fileLoader = new LocalSrcFileLoader(Paths.get("./"));
        UUID uuid;
        if (!fileLoader.exists(uuidFileName)) {
            LOGGER.trace("File creation...");
            fileLoader.createNewFile(uuidFileName);
            LOGGER.trace("{} file created", uuidFileName);
            uuid = createNewUuid();
        } else {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileLoader.loadAsStream(uuidFileName), StandardCharsets.UTF_8))) {
                uuid = UUID.fromString(reader.readLine());
                LOGGER.trace("UUID read from existing file");
            } catch (IOException e) {
                LOGGER.trace("Failed to read worker's UUID from {} file", uuidFileName);
                throw new RuntimeException(e);
            }
        }
        return uuid;
    }

    private static UUID createNewUuid() {
        UUID uuid = UUID.randomUUID();
        try (BufferedWriter fileWriter = Files.newBufferedWriter(Path.of(uuidFileName),StandardCharsets.UTF_8)) {
            fileWriter.write(uuid + "\n");
            fileWriter.flush();
            LOGGER.trace("UUID generated");
        } catch (IOException e) {
            LOGGER.trace("Failed to write worker's UUID into {} file", uuidFileName);
            throw new RuntimeException(e);
        }
        return uuid;
    }


}
