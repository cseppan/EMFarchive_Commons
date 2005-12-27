package gov.epa.emissions.commons.io.importer;

import java.sql.SQLException;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.TableFormat;

public class DataTable {

    public String format(String datasetName) {
        String result = datasetName;

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

    public void create(String table, Datasource datasource, TableFormat tableFormat) throws ImporterException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        try {
            tableDefinition.createTable(table, tableFormat.cols());
        } catch (SQLException e) {
            throw new ImporterException("could not create table - " + table + "\n" + e.getMessage(), e);
        }
    }

    public void drop(String table, Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.dropTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

}
