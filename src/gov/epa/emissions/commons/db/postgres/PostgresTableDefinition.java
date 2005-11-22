package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.db.TableDefinitionDelegate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class PostgresTableDefinition implements TableDefinition {

    private String schema;

    private TableDefinitionDelegate delegate;

    protected PostgresTableDefinition(String schema, Connection connection) {
        this.schema = schema;
        this.delegate = new TableDefinitionDelegate(connection);
    }

    public List getTableNames() throws SQLException {
        return delegate.getTableNames();
    }

    /**
     * Create the table using the header with multiple primary colums NOTE:
     * please ensure that primaryCols is a subset of colNames before calling
     * this method
     */
    public void createTableWithOverwrite(String table, String[] colNames, String[] colTypes, String[] primaryCols)
            throws SQLException {
        int length = colNames.length;
        if (length != colTypes.length)
            throw new SQLException("There are different numbers of column names and types");

        deleteTable(table);

        String queryString = "CREATE TABLE " + qualified(table) + " (";

        for (int i = 0; i < length - 1; i++) {
            queryString += clean(colNames[i]) + " " + colTypes[i] + ", ";
        }// for i
        queryString += clean(colNames[length - 1]) + " " + colTypes[length - 1];

        String primaryColumns = "";
        if (primaryCols != null && primaryCols.length != 0) {
            primaryColumns = ", PRIMARY KEY (";
            for (int i = 0; i < primaryCols.length - 1; i++) {
                primaryColumns += clean(primaryCols[i]) + ", ";
            }
            primaryColumns += clean(primaryCols[primaryCols.length - 1]) + " )";
        }
        queryString = queryString + primaryColumns + ")";

        execute(queryString);
    }

    // TODO: verify if the CREATE TABLE syntax is applicable to Postgres
    public void createTable(String table, String[] colNames, String[] colTypes, String primaryCol) throws SQLException {
        if (colNames.length != colTypes.length)
            throw new SQLException("There are different numbers of column names and types");

        String ddlStatement = "CREATE TABLE " + qualified(table) + " (";

        for (int i = 0; i < colNames.length; i++) {
            // one of the columnnames was "dec" for december.. caused a problem
            // there
            if (colNames[i].equals("dec"))
                colNames[i] = colNames[i] + "1";

            ddlStatement = ddlStatement + clean(colNames[i]) + " " + colTypes[i]
                    + (colNames[i].equals(primaryCol) ? " PRIMARY KEY " + ", " : ", ");
        }// for i
        ddlStatement = ddlStatement.substring(0, ddlStatement.length() - 2) + ")";

        execute(ddlStatement);
    }

    public void deleteTable(String table) {
        try {
            execute("DROP TABLE " + qualified(table));
        } catch (SQLException e) {
            System.err.println("Table " + qualified(table) + " could not be dropped");
        }
    }

    private String qualified(String table) {
        return schema + "." + table;
    }

    public boolean tableExists(String table) throws Exception {
        return delegate.tableExist(table);
    }

    public void addIndex(String table, String indexName, String[] indexColumnNames) throws SQLException {
        StringBuffer query = new StringBuffer();
        // postgres indexes must be unique across tables/database
        String syntheticIndexName = table.replace('.', '_') + "_" + indexName;
        query.append("CREATE INDEX " + syntheticIndexName + " ON " + qualified(table) + " (" + indexColumnNames[0]);
        for (int i = 1; i < indexColumnNames.length; i++) {
            query.append(", " + indexColumnNames[i]);
        }
        query.append(")");

        execute(query.toString());
    }

    public void addColumn(String table, String columnName, String columnType, String afterColumnName) throws Exception {
        String statement = "ALTER TABLE " + qualified(table) + " ADD " + columnName + " " + columnType;
        execute(statement);
    }

    private void execute(final String query) throws SQLException {
        delegate.execute(query);
    }

    private String clean(String data) {
        return (data).replace('-', '_');
    }

    public void createTable(String table, DbColumn[] cols) throws SQLException {
        String queryString = "CREATE TABLE " + qualified(table) + " (";

        for (int i = 0; i < cols.length - 1; i++) {
            queryString += clean(cols[i].name()) + " " + cols[i].sqlType() + ", ";
        }// for i
        queryString += clean(cols[cols.length - 1].name()) + " " + cols[cols.length - 1].sqlType();

        queryString = queryString + ")";
        execute(queryString);
    }

}
