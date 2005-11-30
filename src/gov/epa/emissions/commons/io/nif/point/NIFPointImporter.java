package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NewImporter;
import gov.epa.emissions.commons.io.nif.NIFImporter;


public class NIFPointImporter implements NewImporter{

    private NIFImporter delegate;

    public NIFPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(dataset,new NIFPointDatasetTypeUnits(sqlDataTypes), datasource);
    }
    
    public void preImport() throws ImporterException {
        delegate.preImport();
    }

    public void run() throws ImporterException {
        delegate.run();
    }

    public InternalSource[] internalSources() {
        return delegate.internalSources();
    }

}
