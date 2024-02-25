package it.polimi.ds.map_reduce.utils.database;

/**
 * A database connection parameters
 *
 * @param driver database driver
 * @param jdbcUrl database jdbc's url
 * @param user database username
 * @param password database password
 */
public record ConnectionParams(String driver, String jdbcUrl, String user, String password) {
}
