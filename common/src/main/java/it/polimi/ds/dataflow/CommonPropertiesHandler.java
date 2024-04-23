package it.polimi.ds.dataflow;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Pattern;

public class CommonPropertiesHandler implements PropertiesHandler {

    private static final String ENV_VAR_PREFIX = "DATAFLOW_";

    protected final Properties props;

    public CommonPropertiesHandler(Path defaultPropertiesFilePath, String password) throws IOException {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(password);
        this.props = new EncryptableProperties(encryptor);

        try (Reader r = Files.newBufferedReader(defaultPropertiesFilePath, StandardCharsets.UTF_8)) {
            props.load(r);
        } catch (NoSuchFileException ex) {
            // File does not exist, stuff might still be loaded from system props or env vars
        }
    }

    protected String getProperty(String screamingSnakeCase) {
        final var camelCase = Pattern.compile("_(?<letter>[a-z])")
                .matcher(screamingSnakeCase.toLowerCase(Locale.ROOT))
                .replaceAll(m -> m.group("letter").toUpperCase(Locale.ROOT));

        String prop;
        if ((prop = props.getProperty(screamingSnakeCase)) != null)
            return prop;

        if ((prop = System.getProperty(camelCase)) != null)
            return prop;

        if ((prop = System.getenv(ENV_VAR_PREFIX + screamingSnakeCase)) != null)
            return prop;

        return "";
    }

    @Override
    public String getPgPassword() {
        return getProperty("PG_PASSWORD");
    }

    @Override
    public String getPgUser() {
        return getProperty("PG_USER");
    }

    @Override
    public String getPgUrl() {
        return getProperty("PG_URL");
    }

    @Override
    public String getLocalDfsName() {
        return getProperty("DFS_NODE_NAME");
    }
}
