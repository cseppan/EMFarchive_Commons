package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;


public class NIFNonPointImporter implements Importer{

    private NIFImporter delegate;

    public NIFNonPointImporter(File[] files, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        delegate = new NIFImporter(files, dataset, new NIFNonPointDatasetTypeUnits(sqlDataTypes), datasource);
    }
    
    public void run() throws ImporterException {
        delegate.run();
    }

    public InternalSource[] internalSources() {
        return delegate.internalSources();
    }

    

}
