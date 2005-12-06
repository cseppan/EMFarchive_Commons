package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ModelReadyEmissionsFilesImporter extends AbstractExternalFilesImporter {

    public ModelReadyEmissionsFilesImporter(File folder, String filePattern, Dataset dataset) throws ImporterException {
        super(folder, filePattern, dataset);
        importerName = "Model Ready Emissions Files Importer";
    }

}
