package it.polimi.ds.dataflow.coordinator.properties;

import it.polimi.ds.dataflow.src.WorkDirFileLoader;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;


public class CoordinatorPropertiesHandlerImpl implements CoordinatorPropertiesHandler {
    private static final String DEFAULT_PROPERTIES_FILE_NAME = "coordinator.properties";
    private final Properties props;

    @SuppressFBWarnings("HARD_CODE_PASSWORD")
    public CoordinatorPropertiesHandlerImpl(WorkDirFileLoader fileLoader) throws IOException {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword("jasypt");
        this.props = new EncryptableProperties(encryptor);
        Path defaultPropertiesFilePath = fileLoader.resolvePath(DEFAULT_PROPERTIES_FILE_NAME);
        try (Reader r = Files.newBufferedReader(defaultPropertiesFilePath, StandardCharsets.UTF_8)) {
            props.load(r);
        }
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
    public String getLocalDfsName() {
        return props.getProperty("DFS_NODE_NAME");
    }

    @Override
    public int getListeningPort() {
        return Integer.parseInt(props.getProperty("LISTENING_PORT"));
    }
}
