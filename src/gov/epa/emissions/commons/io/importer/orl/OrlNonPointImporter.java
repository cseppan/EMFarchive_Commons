package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class OrlNonPointImporter {

    private OrlImporter delegate;

    public OrlNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        ColumnsMetadata cols = new OrlPointColumnsMetadata(sqlDataTypes);
        delegate = new OrlImporter(datasource, cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        delegate.run(file, dataset);
    }

}
