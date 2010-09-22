package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.version.Version;

public class VersionedExportStatement implements ExportStatement {

    private VersionedDatasetQuery query;

    public VersionedExportStatement(Version version, Dataset dataset) {
        this.query = new VersionedDatasetQuery(version, dataset);
    }

    public String generate(String table, String rowFilters) {
        return query.generate(table, rowFilters);
    }
}
