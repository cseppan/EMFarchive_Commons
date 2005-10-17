package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NewImporter;

import java.io.File;

public class OrlOnRoadImporter implements NewImporter {

    private OrlImporter delegate;

    public OrlOnRoadImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        ColumnsMetadata cols = new OrlOnRoadColumnsMetadata(sqlDataTypes);
        delegate = new OrlImporter(datasource, cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        delegate.run(file, dataset);
    }

}