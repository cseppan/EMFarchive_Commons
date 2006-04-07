package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableMetaData {

    private Connection connection;

    private String schema;

    private JdbcToCommonsSqlTypeMap typeMaps;

    public TableMetaData(Datasource datasource) {
        this.connection = datasource.getConnection();
        this.schema = datasource.getName();
        this.typeMaps = new JdbcToCommonsSqlTypeMap(datasource.getSqlDataType());
    }

    public Column[] getColumns(String table) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        // postgres driver creates table with lower case lettes and case sensitive
        ResultSet rs = meta.getColumns(null, schema, table.toLowerCase(), null);

        List cols = new ArrayList();
        try {
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                String type = typeMaps.get(jdbcType);
                cols.add(new Column(name, type));
            }
        } finally {
            rs.close();
        }
        if (cols.isEmpty()) {
            throw new SQLException("No columns found in table '" + table + "'");
        }

        return (Column[]) cols.toArray(new Column[0]);
    }

}
