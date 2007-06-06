package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TableModifier {

    protected Connection connection;

    protected String schema;

    protected Column[] columns;

    protected Statement statement;

    protected String tableName;

    public TableModifier(Datasource datasource, String tableName) throws SQLException {
        this.connection = datasource.getConnection();
        this.schema = datasource.getName();
        this.tableName = tableName;
        this.columns = new TableMetaData(datasource).getColumns(tableName);
        this.statement = connection.createStatement();
    }

    public void insert(String[] data) throws Exception {
        if (data.length > columns.length) {
            throw new Exception("Invalid number of data tokens - " + data.length + ". Number of columns in the table: "
                    + columns.length);
        }
        insertRow(tableName, data, columns);
    }

    public void close() throws SQLException {
        statement.close();
    }

    public void insertOneRow(String[] data) throws Exception {
        if (data.length > columns.length) {
            throw new Exception("Invalid number of data tokens - " + data.length + ". Number of columns in the table: "
                    + columns.length);
        }

        try {
            insertRow(tableName, data, columns);
        } finally {
            close();
        }
    }

    private String qualified(String table) {
        return schema + "." + table;
    }

    private void insertRow(String table, String[] data, Column[] cols) throws SQLException {
        StringBuffer insert = createInsertStatement(table, data, cols);
        execute(insert.toString());
    }

    protected StringBuffer createInsertStatement(String table, String[] data, Column[] cols) throws SQLException {
        if (data.length > cols.length)
            throw new SQLException("Invalid number of data tokens - " + data.length + ". Max: " + cols.length);

        StringBuffer insert = new StringBuffer();
        insert.append("INSERT INTO " + qualified(table) + " VALUES(");

        for (int i = 0; i < data.length; i++) {
            if (isTypeString(cols[i])) {
                if (cols[i].name().equalsIgnoreCase("POLL")) {
                    data[i] = data[i].toUpperCase();
                    if (data[i].equals("PM10-PRI"))
                        data[i] = "PM10";
                    else if (data[i].equals("PM25-PRI"))
                        data[i] = "PM2_5";
                } else if (cols[i].name().equalsIgnoreCase("FIPS")) {
                    // add a leading zero if it is missing
                    if (data[i].trim().length() == 4)
                        data[i] = "0" + data[i];
                }
                //make sure the value is not to big....
                if (cols[i].sqlType().toUpperCase().startsWith("VARCHAR")) {
                    if (data[i].trim().length() > cols[i].width() && cols[i].width() > 0) {
                        data[i] = data[i].trim().substring(0, cols[i].width());
                    }
                }
                data[i] = escapeString(data[i]);
            } else {
                if ((data[i] == null || (data[i].trim().length() == 0)))
                    data[i] = "DEFAULT";
                else if (isTypeTimeStamp(cols[i]))
                    data[i] = escapeString(data[i]) + "::" + cols[i].sqlType();
            }

            insert.append(data[i]);
            if (i < (data.length - 1))
                insert.append(',');
        }

        insert.append(')');// close parentheses around the query
        return insert;
    }

    private boolean isTypeTimeStamp(DbColumn column) {
        String sqlType = column.sqlType().toUpperCase();
        return sqlType.startsWith("TIMESTAMP");
    }

    private boolean isTypeString(DbColumn column) {
        String sqlType = column.sqlType().toUpperCase();
        return sqlType.startsWith("VARCHAR") || sqlType.equalsIgnoreCase("TEXT");
    }

    private String escapeString(String val) {
        if (val == null)
            return "''";

        val = val.trim();
        String cleaned = val.replaceAll("\'", "''");
        return "'" + cleaned + "'";
    }

    private void execute(String query) throws SQLException {
        statement.execute(query);
    }

    public void dropData(String key, long value) throws SQLException {
        execute("DELETE FROM " + qualified(tableName) + " WHERE " + key + " = " + value);
    }

    public void dropAllData() throws SQLException {
        execute("DELETE FROM " + qualified(tableName));
    }

}
