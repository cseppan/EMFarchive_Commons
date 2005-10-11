package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.ConnectionParams;

import java.sql.Connection;
import java.sql.SQLException;

import org.postgresql.jdbc3.Jdbc3PoolingDataSource;

public class PostgresConnectionFactory implements ConnectionFactory {

    private Jdbc3PoolingDataSource source;

    public PostgresConnectionFactory(ConnectionParams params) {
        source = new Jdbc3PoolingDataSource();
        source.setServerName(params.getHost());
        source.setDatabaseName(params.getDbName());
        source.setUser(params.getUsername());
        source.setPassword(params.getPassword());
        source.setMaxConnections(10);
    }

    public Connection getConnection() throws SQLException {
        return source.getConnection();
    }
}
