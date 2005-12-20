package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ExternalFilesImporter extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ExternalFilesImporter.class);

    public ExternalFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataType) throws ImporterException {
        super(folder, filePatterns, dataset, datasource, sqlDataType);
        importerName = "External Files Importer";
        log.debug("Default External (Other) Files importer created");
    }

}
