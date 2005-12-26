package gov.epa.emissions.commons.io;

public class VersionedExportStatement implements ExportStatement {

    private int version;

    public VersionedExportStatement(int version) {
        this.version = version;
    }

    public String generate(String table) {
        return "SELECT * FROM " + table + " WHERE version=" + version;
    }

}
