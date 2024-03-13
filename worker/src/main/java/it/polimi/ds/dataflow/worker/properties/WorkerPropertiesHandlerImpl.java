package it.polimi.ds.dataflow.worker.properties;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.UUID;

public final class WorkerPropertiesHandlerImpl implements WorkerPropertiesHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerPropertiesHandlerImpl.class);
    private final Path defaultPropertiesFilePath;
    private final Properties props;

    public WorkerPropertiesHandlerImpl(WorkDirFileLoader fileLoader) throws IOException {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("jasypt");
        this.props = new EncryptableProperties(encryptor);
        defaultPropertiesFilePath = fileLoader.resolvePath("worker.properties");
        try (Reader r = Files.newBufferedReader(defaultPropertiesFilePath, StandardCharsets.UTF_8)) {
            props.load(r);
        }
    }

    @Override
    public UUID getUuid() {
        UUID uuid;
        if (props.getProperty("UUID") != null && !props.getProperty("UUID").isEmpty()) {
            uuid = UUID.fromString(props.getProperty("UUID"));
        } else {
            uuid = UUID.randomUUID();
            props.setProperty("UUID", uuid.toString());
            try (Writer w = Files.newBufferedWriter(defaultPropertiesFilePath, StandardCharsets.UTF_8)) {
                props.store(w, null);
            } catch (IOException e) {
                LOGGER.error("Couldn't save properties");
                throw new RuntimeException(e);
            }
        }
        return uuid;
    }

    @Override
    public String getPgPassword() {
        return props.getProperty("PG_PASSWORD");
    }

    @Override
    public String getPgUser() {
        return props.getProperty("PG_USER");
    }

    @Override
    public String getPgUrl() {
        return props.getProperty("PG_URL");
    }

    @Override
    public String getDfsCoordinatorName() {
        return props.getProperty("DFS_COORDINATOR_NAME") != null
                ? props.getProperty("DFS_COORDINATOR_NAME")
                : "";
    }

    //All workers in this way have the same name since the config file it's only one, this shouldn't be a problem
    //because workers are also uniquely identified through their socket, and during the deployment we can set up different
    //config files for each worker that we intend to use
    @Override
    public String getLocalDfsName() {
        return props.getProperty("DFS_NODE_NAME") != null
                ? props.getProperty("DFS_NODE_NAME")
                : "";
    }

    @Override
    public String getCoordinatorIp() {
        return props.getProperty("COORDINATOR_IP");
    }

    @Override
    public int getCoordinatorPort() {
        return Integer.parseInt(props.getProperty("COORDINATOR_PORT"));
    }

}
