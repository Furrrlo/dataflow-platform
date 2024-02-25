package it.polimi.ds.map_reduce.utils.database;

import javax.naming.ServiceUnavailableException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@SuppressWarnings("unused") //TODO: remove
public class DBConnectionHandler {
    private final Connection connection;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String driver;

    public DBConnectionHandler() throws ServiceUnavailableException {
        Connection connection1;
        jdbcUrl = "jdbc:postgresql://localhost:5432/filesystem";
        username = "postgres";
        password = "admin";
        driver = "org.postgresql.Driver";

        try {
            Class.forName(driver);
            connection1 = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new ServiceUnavailableException("Can't load database driver");
        } catch (SQLException e) {
            throw new ServiceUnavailableException("Couldn't get db connection");
        }
        connection = connection1;
    }

    public DBConnectionHandler(String jdbcUrl, String username, String password, String driver) throws ServiceUnavailableException {
        Connection connection1;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.driver = driver;

        try {
            Class.forName(driver);
            connection1 = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new ServiceUnavailableException("Can't load database driver");
        } catch (SQLException e) {
            throw new ServiceUnavailableException("Couldn't get db connection");
        }
        connection = connection1;
    }

    public final Connection getConnection() {
        return this.connection;
    }

    public void close() throws SQLException {
        if (this.connection != null) {
            connection.close();
        }
    }
}
