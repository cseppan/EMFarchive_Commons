package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAPointImporter implements Importer {

    private IDAImporter delegate;

    public IDAPointImporter(File file, Dataset dataset, Datasource emissionDatasource, Datasource referenceDatasource,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        delegate = new IDAImporter(dataset, emissionDatasource, referenceDatasource, sqlDataTypes);
        IDAFileFormat fileFormat = new IDAPointFileFormat(sqlDataTypes);
        delegate.setup(file, fileFormat);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
