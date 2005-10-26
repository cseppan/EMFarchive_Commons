package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLNonPointImporter implements Importer {

	private ORLImporter delegate;
    
    
	public ORLNonPointImporter(Datasource datasource) {
        delegate = new ORLImporter(datasource);
	}

	public void preCondition(File folder, String filePattern) {
		delegate.preCondition(folder, filePattern);
	}

	public void run(Dataset dataset) throws ImporterException {
        DatasetType datasetType = dataset.getDatasetType();
        FormatUnit [] units = datasetType.getFormatUnits();
        delegate.run(dataset,units[0]);
	}
}
