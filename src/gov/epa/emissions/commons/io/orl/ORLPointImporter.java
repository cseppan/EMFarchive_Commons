package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewImporter;
import gov.epa.emissions.commons.io.OptionalColumnsMetadata;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLPointImporter implements NewImporter {

    private ORLImporter delegate;

    public ORLPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        OptionalColumnsMetadata cols = new ORLPointColumnsMetadata(sqlDataTypes);
        delegate = new ORLImporter(datasource, cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        delegate.run(file, dataset);
    }

}
