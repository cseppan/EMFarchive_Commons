package gov.epa.emissions.framework.db;

import gov.epa.emissions.commons.db.Datasource;

public class VersionsReader {

    private Datasource datasource;

    public VersionsReader(Datasource datasource) {
        this.datasource = datasource;
    }

    public int[] fetch(int datasetId, int version) {
        return new int[] { 0 };
    }

}
