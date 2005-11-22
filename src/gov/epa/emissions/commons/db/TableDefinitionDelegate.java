package gov.epa.emissions.commons.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableDefinitionDelegate {

    private Connection connection;

    public TableDefinitionDelegate(Connection connection) {
        this.connection = connection;
    }

    public List getTableNames() throws SQLException {
        List tableNames = new ArrayList();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, null, new String[] { "TABLE" });
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            tableNames.add(tableName);
        }
        return tableNames;
    }

    public boolean tableExist(String table) throws SQLException {
        List tableNames = getTableNames();
        for (int i = 0; i < tableNames.size(); i++) {
            String name = (String) tableNames.get(i);
            if (name.equalsIgnoreCase(table))
                return true;
        }
        return false;
    }

    public void execute(String query) throws SQLException {
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(query);
        } finally {
            if (statement != null)
                statement.close();
        }
    }

}
