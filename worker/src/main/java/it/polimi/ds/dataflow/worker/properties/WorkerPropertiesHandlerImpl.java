package it.polimi.ds.dataflow.worker.properties;

import it.polimi.ds.dataflow.CommonPropertiesHandler;
import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public final class WorkerPropertiesHandlerImpl extends CommonPropertiesHandler implements WorkerPropertiesHandler {

    private static final String DEFAULT_PROPERTIES_FILE_NAME = "worker.properties";

    private final Path defaultPropertiesFilePath;

    public WorkerPropertiesHandlerImpl(WorkDirFileLoader fileLoader) throws IOException {
        super(fileLoader.resolvePath(DEFAULT_PROPERTIES_FILE_NAME), "jasypt");
        defaultPropertiesFilePath = fileLoader.resolvePath(DEFAULT_PROPERTIES_FILE_NAME);
    }

    @Override
    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CHECKED")
    public UUID getUuid() {
        String candidate = getProperty("UUID");
        if(!candidate.isEmpty())
            return UUID.fromString(candidate);

        UUID uuid = UUID.randomUUID();
        props.setProperty("UUID", uuid.toString());
        try (Writer w = Files.newBufferedWriter(defaultPropertiesFilePath, StandardCharsets.UTF_8)) {
            props.store(w, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Couldn't save properties", e);
        }
        return uuid;
    }

    @Override
    public String getDfsCoordinatorName() {
        return getProperty("DFS_COORDINATOR_NAME");
    }

    @Override
    public String getCoordinatorIp() {
        return getProperty("COORDINATOR_IP");
    }

    @Override
    public int getCoordinatorPort() {
        return Integer.parseInt(getProperty("COORDINATOR_PORT"));
    }
}
