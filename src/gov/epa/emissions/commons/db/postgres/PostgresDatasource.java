package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;

import java.sql.Connection;

public class PostgresDatasource implements Datasource {

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

}
