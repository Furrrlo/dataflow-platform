package it.polimi.ds.map_reduce.utils.database;

import javax.naming.ServiceUnavailableException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBStaticConnectionHandler {
    private static final String defaultJdbcUrl = "jdbc:postgresql://localhost:5432/filesystem";
    private static final String defaultUsername = "postgres";
    private static final String defaultPassword = "admin";
    private static final String defaultDriver = "org.postgresql.Driver";

    /**
     * Returns a db connection with default parameters
     *
     * @return the connection to the db
     * @throws ServiceUnavailableException either if database's driver
     *                                     couldn't be loaded or database is unreachable
     */
    public static Connection getConnection() throws ServiceUnavailableException {
        Connection connection;
        try {
            Class.forName(defaultDriver);
            connection = DriverManager.getConnection(defaultJdbcUrl, defaultUsername, defaultPassword);
        } catch (ClassNotFoundException e) {
            throw new ServiceUnavailableException("Can't load database driver");
        } catch (SQLException e) {
            throw new ServiceUnavailableException("Couldn't get database connection");
        }
        return connection;
    }

    /**
     * Returns a db connection with the parameters specified
     * in a {@link ConnectionParams} record
     *
     * @param ctx record containing the connection's parameters
     * @return the connection to the db
     * @throws ServiceUnavailableException either if database's driver
     *                                     couldn't be loaded or database is unreachable
     */
    @SuppressWarnings("unused") //TODO: remove
    public static Connection getConnection(ConnectionParams ctx) throws ServiceUnavailableException {
        Connection connection;

        String driver = ctx.driver();
        String url = ctx.jdbcUrl();
        String user = ctx.user();
        String password = ctx.password();
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            throw new ServiceUnavailableException("Can't load database driver");
        } catch (SQLException e) {
            throw new ServiceUnavailableException("Couldn't get database connection");
        }
        return connection;
    }

    /**
     * Closes the connection passed as parameter
     *
     * @param connection connection to be closed
     * @throws SQLException if the connection closure fails
     */
    @SuppressWarnings("unused") //TODO: remove
    public static void closeConnection(Connection connection) throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
