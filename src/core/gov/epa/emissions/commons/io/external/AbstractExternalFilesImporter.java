package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExternalSource;
import gov.epa.emissions.commons.io.importer.FilesFromPattern;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public abstract class AbstractExternalFilesImporter implements Importer {

    protected String importerName;

    private File[] files;

    private Dataset dataset;

    public AbstractExternalFilesImporter(File folder, String[] filePatterns, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataType) throws ImporterException {
        this(folder, filePatterns, dataset, dbServer, sqlDataType, null);
    }

    public AbstractExternalFilesImporter(File folder, String[] filePatterns, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        this.dataset = dataset;
        files = new FilesFromPattern(folder, filePatterns, dataset).files();
        importerName = "Abstract External Files Importer";
    }

    public void run() {
        ExternalSource extSrc = null;

        for (int i = 0; i < files.length; i++) {
            extSrc = new ExternalSource(files[i].getAbsolutePath());
            extSrc.setListindex(i);
            dataset.addExternalSource(extSrc);
        }
    }

}
