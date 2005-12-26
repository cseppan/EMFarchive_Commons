package gov.epa.emissions.commons.io;

public class SimpleExportStatement implements ExportStatement {

    public String generate(String table) {
        return "SELECT * FROM " + table;
    }

}
