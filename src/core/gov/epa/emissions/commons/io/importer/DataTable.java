package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableCreator;
import gov.epa.emissions.commons.io.TableFormat;

public class DataTable {

    private String name;

    private TableCreator delegate;
    
    public DataTable(Dataset dataset, Datasource datasource) {
        this.name = createName(dataset.getName());
        this.delegate = new TableCreator(datasource);
    }

    public String name() {
        return name;
    }

    public String createName(String result) {
        for (int i = 0; i < result.length(); i++) {
            if (!Character.isLetterOrDigit(result.charAt(i))) {
                result = result.replace(result.charAt(i), '_');
            }
        }

        if (Character.isDigit(result.charAt(0))) {
            result = result.replace(result.charAt(0), '_');
            result = "DS" + result;
        }
        return result.trim().replaceAll(" ", "_");
    }

    public static String encodeTableName(String tableName) {
        for (int i = 0; i < tableName.length(); i++) {
            if (!Character.isLetterOrDigit(tableName.charAt(i))) {
                tableName = tableName.replace(tableName.charAt(i), '_');
            }
        }

        if (Character.isDigit(tableName.charAt(0))) {
            tableName = tableName.replace(tableName.charAt(0), '_');
            tableName = "DS" + tableName;
        }
        return tableName.trim().replaceAll(" ", "_");
    }

    public void create(String table, TableFormat tableFormat) throws ImporterException {
        try {
            delegate.create(table, tableFormat);
        } catch (Exception e) {
            throw new ImporterException(e.getMessage());
        }
    }

    public void create(TableFormat tableFormat) throws ImporterException {
        create(name(), tableFormat);
    }

    public void drop(String table) throws ImporterException {
        try {
            delegate.drop(table);
        } catch (Exception e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    public void drop() throws ImporterException {
        drop(name());
    }

    public void rename(String oldName, String newName) throws ImporterException {
        try {
            delegate.rename(oldName, newName);
        } catch (Exception e) {
            throw new ImporterException("could not rename table " + name + ", " + e.getMessage());
        }
    }
    
    public boolean exists(String table) throws Exception {
        return delegate.exists(table);
    }

}
