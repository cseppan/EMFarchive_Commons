package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.db.mysql.MySqlDbServer;
import gov.epa.emissions.commons.db.postgres.PostgresConnectionFactory;
import gov.epa.emissions.commons.db.postgres.PostgresDbServer;

import java.sql.SQLException;
import java.util.Properties;

public class DatabaseSetup {

    private DbServer dbServer;

    private String dbType;

    public DatabaseSetup(Properties pref) throws SQLException {
        dbType = pref.getProperty("database.type");

        String emissionsDatasource = pref.getProperty("datasource.emissions.name");
        String referenceDatasource = pref.getProperty("datasource.reference.name");

        String dbName = pref.getProperty("database.name");
        String host = pref.getProperty("database.host");
        String port = pref.getProperty("database.port");
        String username = pref.getProperty("database.username");
        String password = pref.getProperty("database.password");

        if (isMySql()) {
            ConnectionParams emissionParams = new ConnectionParams(emissionsDatasource, host, port, username, password);
            ConnectionParams referenceParams = new ConnectionParams(referenceDatasource, host, port, username, password);
            createMySqlDbServer(emissionParams, referenceParams);
        } else {
            ConnectionParams params = new ConnectionParams(dbName, host, port, username, password);
            createPostgresDbServer(emissionsDatasource, referenceDatasource, params);
        }
    }

    private boolean isMySql() {
        return dbType.equals("mysql");
    }

    private void createPostgresDbServer(String emissionsDatasource, String referenceDatasource, ConnectionParams params)
            throws SQLException {
        PostgresConnectionFactory factory = PostgresConnectionFactory.instance(params);
        dbServer = new PostgresDbServer(factory.getConnection(), referenceDatasource, emissionsDatasource);
    }

    private void createMySqlDbServer(ConnectionParams emissionParams, ConnectionParams referenceparams) throws SQLException {
        dbServer = new MySqlDbServer(emissionParams,referenceparams);
    }

    public DbServer getDbServer() {
        return dbServer;
    }

    public void tearDown() throws SQLException {
        dbServer.disconnect();
    }

    public TableReader tableReader(Datasource datasource) {
        if (isMySql())
            return new MySqlTableReader(datasource.getConnection());

        return new PostgresTableReader(datasource.getConnection());
    }
}
