package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.version.Version;

public class VersionedExportStatement implements ExportStatement {

    private VersionedDatasetQuery query;

    public VersionedExportStatement(Version version) {
        this.query = new VersionedDatasetQuery(version);
    }

    public String generate(String table) {
        return query.generate(table);
    }
}
