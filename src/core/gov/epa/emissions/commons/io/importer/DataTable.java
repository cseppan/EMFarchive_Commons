package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.TableFormat;

import java.sql.SQLException;

public class DataTable {

    private Dataset dataset;

    private Datasource datasource;

    public DataTable(Dataset dataset, Datasource datasource) {
        this.dataset = dataset;
        this.datasource = datasource;
    }

    public String name() {
        String result = dataset.getName();

        for (int i = 0; i < result.length(); i++) {
            if (!Character.isJavaLetterOrDigit(result.charAt(i))) {
                result = result.replace(result.charAt(i), '_');
            }
        }

        if (Character.isDigit(result.charAt(0))) {
            result = result.replace(result.charAt(0), '_');
            result = "DS" + result;
        }

        return result.trim().replaceAll(" ", "_");
    }

    public void create(String table, TableFormat tableFormat) throws ImporterException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        checkTableExist(tableDefinition, table);
        try {
            tableDefinition.createTable(table, tableFormat.cols());
        } catch (SQLException e) {
            throw new ImporterException("could not create table - " + table + "\n" + e.getMessage(), e);
        }
    }

    private void checkTableExist(TableDefinition tableDefinition, String table) throws ImporterException {
        try {
            if (tableDefinition.tableExists(table)) {
                throw new ImporterException("Table '" + table + "' is exist in the database");
            }
        } catch (Exception e) {
            throw new ImporterException("Could not check table '" + table + "' exist or not\n" + e.getMessage());
        }

    }

    public void create(TableFormat tableFormat) throws ImporterException {
        create(name(), tableFormat);
    }

    public void drop(String table) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.dropTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    public void drop() throws ImporterException {
        drop(name());
    }

    public boolean exists(String table) throws Exception {
        return datasource.tableDefinition().tableExists(table);
    }

}
