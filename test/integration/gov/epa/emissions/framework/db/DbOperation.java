package gov.epa.emissions.framework.db;

import java.sql.Connection;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;

public class DbOperation {

    protected DatabaseConnection connection;

    public DbOperation(Connection jdbcConnection) {
        connection = new DatabaseConnection(jdbcConnection);

        DatabaseConfig dbUnitConfig = connection.getConfig();
        dbUnitConfig.setFeature(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, true);
    }
}