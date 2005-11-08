package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFNonPointImporter implements Importer{

    private Dataset dataset;
    
    private NIFImporter delegate;
    

    public NIFNonPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.dataset = dataset;
        delegate = new NIFImporter(datasource, new NIFNonPointDatasetTypeUnits(sqlDataTypes));
    }
    
    public void preCondition(File folder, String filePattern) throws Exception {
        delegate.preImport(dataset);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run(dataset);
    }

    

}
