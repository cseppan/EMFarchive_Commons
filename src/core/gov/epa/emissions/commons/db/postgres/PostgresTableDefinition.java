package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.db.TableDefinitionDelegate;
import gov.epa.emissions.commons.io.TableMetadata;

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
 
    public void dropTable(String table) throws Exception {
        try {
            execute("DROP TABLE " + qualified(table));
        } catch (SQLException e) {
            throw new Exception("Table " + qualified(table) + " could not be dropped"+"\n"+e.getMessage());
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
            queryString += clean(cols[i].name()) + " " + cols[i].sqlType();
            if (cols[i].hasConstraints())
                queryString += " " + cols[i].constraints();
            queryString += ", ";
        }// for i
        queryString += clean(cols[cols.length - 1].name()) + " " + cols[cols.length - 1].sqlType();

        queryString = queryString + ")";
        execute(queryString);
    }

    public TableMetadata getTableMetaData(String tableName) throws SQLException {
       return delegate.getTableMetaData(qualified(tableName));
    }

    public int totalRows(String tableName) throws SQLException {
        return delegate.totalRows(qualified(tableName));
    }

    public void renameTable(String table, String newName) throws SQLException {
        String renameQuery = "ALTER TABLE " + qualified(table) + " RENAME TO " + newName;
        execute(renameQuery);
    }

}
