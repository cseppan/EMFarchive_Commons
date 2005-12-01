package gov.epa.emissions.commons.db.mysql;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.TableDefinition;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlDatasource implements Datasource, Cloneable, Serializable {

    private Connection connection;

    private DataModifier dataAcceptor;

    private String name;

    public MySqlDatasource(String name, Connection connection) {
        this.name = name;
        this.connection = connection;
        this.dataAcceptor = new DataModifier(name, connection);
    }

    public String getName() {
        return name;
    }

    public Connection getConnection() {
        return connection;
    }

    public void execute(final String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(query);
    }

    public DataModifier dataModifier() {
        return dataAcceptor;
    }

    public DataQuery query() {
        return new MySqlDataQuery(name, connection);
    }

    public TableDefinition tableDefinition() {
        return new MySqlTableDefinition(name, connection);
    }

}
