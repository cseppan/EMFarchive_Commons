package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.ConnectionParams;

import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

public class PostgresConnectionFactory implements ConnectionFactory {

    private static PostgresConnectionFactory instance;

    private Jdbc3PoolingDataSource source;

    private PostgresConnectionFactory(ConnectionParams params) {
        source = new Jdbc3PoolingDataSource();
        source.setServerName(params.getHost());
        source.setDatabaseName(params.getDbName());
        source.setUser(params.getUsername());
        source.setPassword(params.getPassword());
        source.setMaxConnections(16);
    }

    // NOTE: the only reason why this is a Singleton - need to have
    // a pool of DB connections for the entire test suite. Could not find
    // a simple, intuitive way to do it.
    public static PostgresConnectionFactory instance(ConnectionParams params) {
        if (instance == null)
            instance = new PostgresConnectionFactory(params);

        return instance;
    }

    public Connection getConnection() throws SQLException {
        return source.getConnection();
    }
}
