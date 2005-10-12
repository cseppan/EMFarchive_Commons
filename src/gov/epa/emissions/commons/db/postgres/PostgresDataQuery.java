package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.DataQuery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresDataQuery implements DataQuery {

    private Connection connection;
    private String schema;

    public PostgresDataQuery(String schema, Connection connection) {
        this.schema = schema;
        this.connection = connection;
    }

    public ResultSet executeQuery(String query) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        return statement.executeQuery(query);
    }

    public void execute(String query) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        statement.execute(query);
    }

    // FIXME: duplicate methods in both datasources
    public ResultSet select(String[] columnNames, String table) throws SQLException {
        final String selectPrefix = "SELECT ";
        StringBuffer query = new StringBuffer(selectPrefix);
        query.append(columnNames[0]);
        for (int i = 1; i < columnNames.length; i++) {
            query.append("," + columnNames[i]);
        }
        final String fromSuffix = " FROM " + qualified(table);
        query.append(fromSuffix);

        Statement statement = connection.createStatement();
        statement.execute(query.toString());

        return statement.getResultSet();
    }

    private String qualified(String table) {
        return schema + "." + table;
    }

    public ResultSet selectAll(String table) throws SQLException {
        return select(new String[] { "*" }, table);
    }

}
