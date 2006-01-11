package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAPointImporter implements Importer {

    private IDAImporter delegate;

    public IDAPointImporter(File file, Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) throws ImporterException {
        delegate = new IDAImporter(dataset, dbServer, sqlDataTypes);
        IDAFileFormat fileFormat = new IDAPointFileFormat(sqlDataTypes);
        delegate.setup(file, fileFormat);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
