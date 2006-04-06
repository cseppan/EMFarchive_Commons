package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;

import java.sql.Connection;
import java.sql.SQLException;

public class PostgresDatasource implements Datasource {

    private static final int OPTIMIZED_FETCH_SIZE = 25000;// # of rows

    private Connection connection;

    private DataModifier dataAcceptor;

    private String name;

    public PostgresDatasource(String name, Connection connection, SqlDataTypes types) {
        this.connection = connection;
        this.name = name;
        this.dataAcceptor = new DataModifier(name, connection, types);
    }

    public String getName() {
        return name;
    }

    public Connection getConnection() {
        return connection;
    }

    public DataModifier dataModifier() {
        return dataAcceptor;
    }

    public DataQuery query() {
        return new PostgresDataQuery(name, connection);
    }

    public TableDefinition tableDefinition() {
        return new PostgresTableDefinition(name, connection);
    }

    public OptimizedQuery optimizedQuery(String query) throws SQLException {
        return new OptimizedPostgresQuery(connection, query, OPTIMIZED_FETCH_SIZE);
    }

}
