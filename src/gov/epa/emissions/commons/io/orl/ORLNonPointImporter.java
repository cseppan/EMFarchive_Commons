package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLNonPointImporter implements Importer {

    private ORLImporter delegate;

    public ORLNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        ORLNonPointFileFormat cols = new ORLNonPointFileFormat(sqlDataTypes);
        delegate = new ORLImporter(datasource, cols, sqlDataTypes);
    }

    public void preCondition(File folder, String filePattern) {
        delegate.preCondition(folder, filePattern);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run(dataset);
    }

}
