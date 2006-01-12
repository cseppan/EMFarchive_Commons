package gov.epa.emissions.commons.db;

import gov.epa.emissions.commons.io.Column;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DataModifier {

    protected Connection connection = null;

    private String schema;

    private JdbcToCommonsSqlTypeMap typeMap;

    public DataModifier(String schema, Connection connection, SqlDataTypes types) {
        this.schema = schema;
        this.connection = connection;
        typeMap = new JdbcToCommonsSqlTypeMap(types);
    }

    private void execute(String sql) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Error in executing query-" + sql + "\n" + e.getMessage());
        } finally {
            statement.close();
        }
    }

    /**
     * UPDATE databaseName.tableName SET columnName = setExpr WHERE whereColumns[i] LIKE 'likeClauses[i]'
     * 
     * @param columnName -
     *            the column to update
     * @param setExpr -
     *            the expression used to update the column value
     * @param whereColumns -
     *            left hand sides of LIKE expressions for WHERE
     * @param likeClauses -
     *            right hand sides of LIKE expressions for WHERE
     * @throws Exception
     *             if encounter error updating table
     */
    public void updateWhereLike(String table, String columnName, String setExpr, String[] whereColumns,
            String[] likeClauses) throws Exception {
        if (whereColumns.length != likeClauses.length) {
            throw new Exception("There are different numbers of WHERE column names and LIKE clauses");
        }

        // instantiate a new string buffer in which the query would be created
        StringBuffer sb = new StringBuffer("UPDATE " + qualified(table) + " SET " + columnName + " = " + setExpr
                + " WHERE ");

        // add the first LIKE expression
        sb.append(whereColumns[0] + " LIKE '" + likeClauses[0] + "'");

        // if there is more than one LIKE expression, add
        // "AND" before each of the remaining expressions
        for (int i = 1; i < whereColumns.length; i++) {
            sb.append(" AND " + whereColumns[i] + " LIKE '" + likeClauses[i] + "'");
        }

        execute(sb.toString());
    }// updateWhereLike(String, String, String[], String[])

    /**
     * UPDATE databaseName.tableName SET columnName = setExpr WHERE whereColumns[i] = equalsClauses[i]
     * 
     * @param columnName -
     *            the column to update
     * @param setExpr -
     *            the expression used to update the column value
     * @param whereColumns -
     *            left hand sides of = expressions for WHERE
     * @param equalsClauses -
     *            right hand sides of = expressions for WHERE
     * @throws Exception
     *             if encounter error updating table
     */
    public void updateWhereEquals(String table, String columnName, String setExpr, String[] whereColumns,
            String[] equalsClauses) throws Exception {
        if (whereColumns.length != equalsClauses.length) {
            throw new Exception("There are different numbers of WHERE column names and = clauses");
        }

        // instantiate a new string buffer in which the query would be created
        StringBuffer sb = new StringBuffer("UPDATE " + qualified(table) + " SET " + columnName + " = " + setExpr
                + " WHERE ");

        // add the first LIKE expression
        sb.append(whereColumns[0] + " = " + equalsClauses[0]);

        // if there is more than one LIKE expression, add
        // "AND" before each of the remaining expressions
        for (int i = 1; i < whereColumns.length; i++) {
            sb.append(" AND " + whereColumns[i] + " = " + equalsClauses[i]);
        }

        execute(sb.toString());
    }// updateWhereEquals(String, String, String[], String[])

    /**
     * Generate a concat expression for usage in SQL statements. If the value to be concatenated is a literal
     * (constant), it should be enclosed in ''.
     * 
     * @param exprs -
     *            Array of data ('literals' and column names)
     * @return the SQL concat expression
     */
    // FIXME: does this work with Postgres ?
    public String generateConcatExpr(String[] exprs) {
        StringBuffer concat = new StringBuffer("concat(");
        // add the first string
        concat.append(exprs[0]);
        for (int i = 1; i < exprs.length; i++) {
            concat.append("," + exprs[i]);
        }
        concat.append(")");

        return concat.toString();
    }

    // FIXME: remove, once Importers/Exporters are complete, used by legacy code
    public void insertRow(String table, String[] data, String[] colTypes) throws SQLException {
        StringBuffer insert = new StringBuffer();
        insert.append("INSERT INTO " + qualified(table) + " VALUES(");

        for (int i = 0; i < data.length; i++) {
            if (colTypes[i].startsWith("VARCHAR")) {
                String cleanedCell = data[i].replace('-', '_');
                String cellWithSingleQuotesEscaped = cleanedCell.replaceAll("\'", "''");
                insert.append("'" + cellWithSingleQuotesEscaped + "'");
            } else {
                if (data[i].trim().length() == 0)
                    data[i] = "DEFAULT";
                insert.append(data[i]);
            }
            if (i < (data.length - 1))
                insert.append(',');
        }
        insert.append(')');// close parentheses around the query

        execute(insert.toString());
    }

    /**
     * Use 'insertRow(String table, String[] data) instead.
     */
    public void insertRow(String table, String[] data, DbColumn[] cols) throws SQLException {
        if (data.length > cols.length)
            throw new SQLException("Invalid number of data tokens - " + data.length + ". Max: " + cols.length);

        StringBuffer insert = new StringBuffer();
        insert.append("INSERT INTO " + qualified(table) + " VALUES(");

        for (int i = 0; i < data.length; i++) {
            if ((data[i] == null || (data[i].trim().length() == 0)) && (!isTypeString(cols[i])))
                data[i] = "DEFAULT";
            if (isTypeString(cols[i])) {
                data[i] = escapeAndDelimitStringValue(data[i]);
            }

            insert.append(data[i]);

            if (i < (data.length - 1))
                insert.append(',');
        }

        insert.append(')');// close parentheses around the query

        execute(insert.toString());
    }

    public void insertRow(String table, String[] data) throws SQLException {
        insertRow(table, data, getColumns(table));
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
                String type = typeMap.get(jdbcType);
                cols.add(new Column(name, type));
            }
        } finally {
            rs.close();
        }
        return (Column[]) cols.toArray(new Column[0]);
    }

    private boolean isTypeString(DbColumn column) {
        String sqlType = column.sqlType();
        return sqlType.startsWith("VARCHAR") || sqlType.equalsIgnoreCase("TEXT");
    }

    private String escapeAndDelimitStringValue(String val) {
        if (val == null)
            return "''";

        val = val.trim();
        String cleanedCell = val.replace('-', '_');
        String cellWithSingleQuotesEscaped = cleanedCell.replaceAll("\'", "''");

        return "'" + cellWithSingleQuotesEscaped + "'";
    }

    public void dropData(String table, String key, long value) throws SQLException {
        execute("DELETE FROM " + qualified(table) + " WHERE " + key + " = " + value);
    }

    private String qualified(String table) {
        return schema + "." + table;
    }

    public void dropAll(String table) throws SQLException {
        execute("DELETE FROM " + qualified(table));
    }

}
