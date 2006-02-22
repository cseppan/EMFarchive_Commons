package gov.epa.emissions.commons.db.mysql;

import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.db.TableDefinitionDelegate;
import gov.epa.emissions.commons.io.TableMetadata;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class MySqlTableDefinition implements TableDefinition {

    private String schema;

    private TableDefinitionDelegate delegate;

    public MySqlTableDefinition(String schema, Connection connection) {
        this.schema = schema;
        this.delegate = new TableDefinitionDelegate(connection);
    }

    public List getTableNames() throws SQLException {
        return delegate.getTableNames();
    }

    public void createTableWithOverwrite(String table, String[] colNames, String[] colTypes, String[] primaryCols)
            throws SQLException {
        // check to see if there are the same number of column names and column
        // types
        int length = colNames.length;
        if (length != colTypes.length)
            throw new SQLException("There are different numbers of column names and types");

        dropTable(table);

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

    public void createTable(String table, String[] colNames, String[] colTypes, String primaryCol) throws SQLException {
        if (colNames.length != colTypes.length)
            throw new SQLException("There are different numbers of column names and types");

        String ddlStatement = "CREATE TABLE " + qualified(table) + " (";

        for (int i = 0; i < colNames.length; i++) {
            // one of the columnnames was "dec" for december.. caused a problem
            // there
            if (colNames[i].equalsIgnoreCase("dec"))
                colNames[i] = colNames[i] + "1";

            ddlStatement = ddlStatement + clean(colNames[i]) + " " + colTypes[i]
                    + (colNames[i].equals(primaryCol) ? " PRIMARY KEY " + ", " : ", ");
        }// for i
        ddlStatement = ddlStatement.substring(0, ddlStatement.length() - 2) + ")";
        System.err.println("create table -"+ddlStatement);
        execute(ddlStatement);
    }

    public void deleteTableQuietly(String table) {
        try {
            execute("DROP TABLE IF EXISTS " + qualified(table));
        } catch (SQLException e) {
            System.err.println("Could not delete table - " + table + ". Ignoring..");
        }
    }

    public void dropTable(String table) throws SQLException {
        execute("DROP TABLE " + qualified(table));
    }

    public boolean tableExists(String table) throws SQLException {
        return delegate.tableExist(table);
    }

    public void addIndex(String table, String indexName, String[] indexColumnNames) throws SQLException {
        // instantiate a new string buffer in which the query would be created
        StringBuffer sb = new StringBuffer("ALTER TABLE " + qualified(table) + " ADD ");
        final String INDEX = "INDEX ";

        sb.append(INDEX + indexName + "(" + indexColumnNames[0]);
        for (int i = 1; i < indexColumnNames.length; i++) {
            sb.append(", " + indexColumnNames[i]);
        }
        sb.append(")");

        execute(sb.toString());
    }

    public void addColumn(String table, String columnName, String columnType, String afterColumnName) throws Exception {
        // instantiate a new string buffer in which the query would be created
        StringBuffer sb = new StringBuffer("ALTER TABLE " + qualified(table) + " ADD ");
        final String AFTER = " AFTER ";

        sb.append(columnName + " " + columnType);
        if (afterColumnName != null) {
            sb.append(AFTER + afterColumnName);
        }// if

        execute(sb.toString());
    }

    private String clean(String dirtyStr) {
        return dirtyStr.replace('-', '_');
    }

    private void execute(final String query) throws SQLException {
        delegate.execute(query);
    }

    public void createTable(String table, DbColumn[] cols) throws SQLException {
        String queryString = "CREATE TABLE " + qualified(table) + " (";

        for (int i = 0; i < cols.length - 1; i++) {
            queryString += clean(cols[i].name()) + " " + cols[i].sqlType() + ", ";
        }
        queryString += clean(cols[cols.length - 1].name()) + " " + cols[cols.length - 1].sqlType();

        queryString = queryString + ")";
        execute(queryString);
    }

    private String qualified(String table) {
        return schema + "." + table;
    }

    public TableMetadata getTableMetaData(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

}
