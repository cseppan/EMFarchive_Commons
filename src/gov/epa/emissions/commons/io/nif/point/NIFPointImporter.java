package gov.epa.emissions.commons.io.nif.point;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFPointImporter implements Importer{

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

}
