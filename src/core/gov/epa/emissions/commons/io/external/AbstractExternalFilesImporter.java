package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.ExternalSource;
import gov.epa.emissions.commons.io.importer.FilePatternMatcher;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
        init(folder, filePatterns, dataset);
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

    private void init(File path, String[] filePatterns, Dataset dataset) throws ImporterException {
        DatasetType datasetType = dataset.getDatasetType();
        if (filePatterns.length > 1) {
            throw new ImporterException("Too many parameters for importer: " + importerName
                    + " requires only one file pattern or filename");
        }
        if (filePatterns[0].length() == 0) {
            throw new ImporterException(importerName + " importer requires a file pattern or filename");
        }

        files = null;
        int minFiles = datasetType.getMinfiles();
        files = extractFileNames(path, filePatterns[0]);
        if (files.length < minFiles) {
            throw new ImporterException(importerName + " importer requires " + minFiles + " files");
        }
    }

    private File[] extractFileNames(File folder, String filePattern) throws ImporterException {
        String[] fileNamesInFolder = fileNames(folder);

        FilePatternMatcher matcher = new FilePatternMatcher(filePattern);
        String[] matchedFileNames = matcher.matchingNames(fileNamesInFolder);
        if (matchedFileNames == null || matchedFileNames.length == 0) {
            throw new ImporterException("There are no files found in the directory '" + folder.getAbsolutePath()
                    + "' matching pattern '" + filePattern + "'");
        }
        File[] files = new File[matchedFileNames.length];
        for (int i = 0; i < files.length; i++) {
            files[i] = new File(folder, matchedFileNames[i]);
        }
        return files;
    }

    private String[] fileNames(File folder) throws ImporterException {
        if (!folder.isDirectory()) {
            throw new ImporterException(folder.getAbsolutePath() + " is not a directory");
        }
        List names = new ArrayList();
        String[] fileNames = folder.list();
        for (int i = 0; i < fileNames.length; i++) {
            if (new File(folder, fileNames[i]).isFile()) {
                names.add(fileNames[i]);
            }
        }
        if (names.isEmpty()) {
            throw new ImporterException("There are no files in the directory '" + folder + "'");
        }
        return (String[]) names.toArray(new String[0]);
    }

}
