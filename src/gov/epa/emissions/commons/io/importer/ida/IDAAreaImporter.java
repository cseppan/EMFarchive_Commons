package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAAreaImporter {
	private IDAImporter delegate;

    public IDAAreaImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new IDAImporter(datasource,sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        delegate.run(file, dataset);
    }
	
}
