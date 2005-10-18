package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLNonPointImporter implements NewImporter {

    private ORLImporter delegate;

    public ORLNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        ORLNonPointColumnsMetadata cols = new ORLNonPointColumnsMetadata(sqlDataTypes);
        delegate = new ORLImporter(datasource, cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        delegate.run(file, dataset);
    }

}
