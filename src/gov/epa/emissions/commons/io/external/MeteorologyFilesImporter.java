package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class MeteorologyFilesImporter extends AbstractExternalFilesImporter {

    public MeteorologyFilesImporter(File folder, String filePattern, Dataset dataset) throws ImporterException {
        super(folder, filePattern, dataset);
        importerName = "Meteorology Files Importer";
    }

}
