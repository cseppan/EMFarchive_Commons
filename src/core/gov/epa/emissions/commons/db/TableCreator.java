package gov.epa.emissions.commons.db;

import java.sql.SQLException;

import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.ImporterException;

public class TableCreator {

    private Datasource datasource;

    public TableCreator(Datasource datasource) {
        this.datasource = datasource;
    }

    public void create(String table, TableFormat tableFormat) throws Exception {
        create(table, tableFormat, -1);
    }

    public void create(String table, TableFormat tableFormat, int datasetId) throws Exception {
        TableDefinition tableDefinition = datasource.tableDefinition();
        checkTableExist(tableDefinition, table);
        try {
            if (datasetId != -1)
                tableDefinition.createTable(table, tableFormat.cols(), datasetId);
            else
                tableDefinition.createTable(table, tableFormat.cols());
        } catch (SQLException e) {
            throw new Exception("could not create table - " + table + "\n" + e.getMessage(), e);
        }
    }

    public void rename(String table, String newName) throws Exception {
        TableDefinition tableDefinition = datasource.tableDefinition();
        checkTableExist(tableDefinition, newName);
        try {
            tableDefinition.renameTable(table, newName);
        } catch (SQLException e) {
            throw new Exception("could not rename table - " + table + " to " + newName + "\n" + e.getMessage(), e);
        }

    }

    private void checkTableExist(TableDefinition tableDefinition, String table) throws Exception {
        try {
            if (tableDefinition.tableExists(table)) {
                throw new ImporterException("Table '" + table
                        + "' already exists in the database, and cannot be created");
            }
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }

    }

    /* table name(without the schema name) */
    public void drop(String table) throws Exception {
        TableDefinition def = datasource.tableDefinition();
        def.dropTable(table);
    }

    public boolean exists(String table) throws Exception {
        return datasource.tableDefinition().tableExists(table);
    }

}
