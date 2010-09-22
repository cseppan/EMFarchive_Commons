package gov.epa.emissions.commons.io;

public class SimpleExportStatement implements ExportStatement {

    public String generate(String table, String rowFilters) {
        if (rowFilters.trim().length()>0)
            return "SELECT * FROM " + table + " WHERE " + rowFilters;
        return "SELECT * FROM " + table;
    }
}