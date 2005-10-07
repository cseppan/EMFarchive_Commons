package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataType;

import java.io.File;

public class TemporalProfileImporter {

    private SqlDataType sqlType;

    private Datasource datasource;

    public TemporalProfileImporter(Datasource datasource, SqlDataType sqlType) {
        this.datasource = datasource;
        this.sqlType = sqlType;

    }

    public void run(File file) {
    }

}
