package gov.epa.emissions.commons.io.external;

import java.io.File;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExternalFilesImporter extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ExternalFilesImporter.class);

    public ExternalFilesImporter(File folder, String filePattern, Dataset dataset) throws ImporterException {
        super(folder, filePattern, dataset);
        importerName = "External Files Importer";
        log.debug("Default External (Other) Files importer created");
    }

}
