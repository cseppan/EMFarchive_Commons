package gov.epa.emissions.commons.db.mysql;

import gov.epa.emissions.commons.db.DataQuery;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlDataQuery implements DataQuery {

    private Connection connection;

    public MySqlDataQuery(Connection connection) {
        this.connection = connection;
    }

    public ResultSet executeQuery(String query) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    public void execute(String query) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(query);
    }

    public ResultSet select(String[] columnNames, String table) throws SQLException {
        final String selectPrefix = "SELECT ";
        StringBuffer sb = new StringBuffer(selectPrefix);
        sb.append(columnNames[0]);
        for (int i = 1; i < columnNames.length; i++) {
            sb.append("," + columnNames[i]);
        }
        final String fromSuffix = " FROM " + table;
        sb.append(fromSuffix);

        Statement statement = connection.createStatement();
        statement.execute(sb.toString());
        ResultSet results = statement.getResultSet();

        return results;
    }

    public ResultSet selectAll(String table) throws SQLException {
        return select(new String[] { "*" }, table);
    }
}
