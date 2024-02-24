package it.polimi.ds.map_reduce.worker;

import it.polimi.ds.map_reduce.src.LocalSrcFileLoader;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.UUID;

public class UuidHandler {
    private static final String uuidFileName = "uuid-storage.txt";

    /*private static final Logger LOGGER = LoggerFactory.getLogger(UuidHandler.class);*/ //TODO: Logger da aggiungere?
    public UuidHandler() {
    }

    public static UUID getUUID() {
        final LocalSrcFileLoader fileLoader = new LocalSrcFileLoader(Paths.get("./"));
        UUID uuid;
        if (!fileLoader.exists(uuidFileName)) {
            fileLoader.createNewFile(uuidFileName);
            uuid = createNewUuid();
        } else {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileLoader.loadAsStream(uuidFileName)))) {
                uuid = UUID.fromString(reader.readLine());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return uuid;
    }

    private static UUID createNewUuid() {
        UUID uuid = UUID.randomUUID();
        try (FileWriter fileWriter = new FileWriter(uuidFileName)) {
            fileWriter.write(uuid + "\n");
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return uuid;
    }


}
