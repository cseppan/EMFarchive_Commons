package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NewImporter;
import gov.epa.emissions.commons.io.nif.NIFImporter;

import java.io.File;

public class NIFPointImporter implements NewImporter{

    private NIFImporter delegate;

    public NIFPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(dataset,new NIFPointDatasetTypeUnits(sqlDataTypes), datasource);
    }
    
    public void preCondition(File folder, String filePattern) throws ImporterException {
        delegate.preImport();
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run();
    }

    public InternalSource[] internalSources() {
        return delegate.internalSources();
    }

}
