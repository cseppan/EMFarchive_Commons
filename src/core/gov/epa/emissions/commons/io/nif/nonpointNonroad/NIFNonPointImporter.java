package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

import java.io.File;


public class NIFNonPointImporter implements Importer{

    private NIFImporter delegate;

    public NIFNonPointImporter(File[] files, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        String tablePrefix = new HelpImporter().tableName(dataset.getName());
        delegate = new NIFImporter(files, dataset, new NIFNonPointFileDatasetTypeUnits(files, tablePrefix, sqlDataTypes), datasource);
    }
    
    public void run() throws ImporterException {
        delegate.run();
    }

}
