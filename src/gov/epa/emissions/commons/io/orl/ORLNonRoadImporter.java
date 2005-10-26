package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ORLNonRoadImporter implements Importer {

    private ORLImporter delegate;

    public ORLNonRoadImporter(Datasource datasource) {
        delegate = new ORLImporter(datasource);
    }

    public void run(Dataset dataset) throws ImporterException {
        DatasetType datasetType = dataset.getDatasetType();
        FormatUnit[] units = datasetType.getFormatUnits();

        delegate.run(dataset, units[0]);
    }

    public void preCondition(File folder, String filePattern) {
        delegate.preCondition(folder, filePattern);
    }

}
