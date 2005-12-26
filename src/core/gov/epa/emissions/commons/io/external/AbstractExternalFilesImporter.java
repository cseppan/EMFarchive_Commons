package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.ExternalSource;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractExternalFilesImporter implements Importer {
    private static Log log = LogFactory.getLog(AbstractExternalFilesImporter.class);

    protected String importerName;

    private File[] files;

    private Dataset dataset;

    public AbstractExternalFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataType) throws ImporterException {
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

    private File validateFile(File path, String fileName) throws ImporterException {
        File file = new File(path, fileName);

        if (!file.exists() || !file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new ImporterException("File not found");
        }

        return file;
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

    private File[] extractFileNames(File path, String fileName) throws ImporterException {
        List allFiles = new ArrayList();

        File[] files = null;

        if (fileName.indexOf("*") >= 0) {
            String[] allFilesInFolder = path.list();
            Pattern pat = Pattern.compile(createPattern(fileName));
            for (int i = 0; i < allFilesInFolder.length; i++) {
                String ipFile = allFilesInFolder[i];
                Matcher m = pat.matcher(ipFile);
                if (m.matches()) {
                    allFiles.add(validateFile(path, ipFile));
                }
            }
        } else {
            allFiles.add(validateFile(path, fileName));
        }
        files = (File[]) allFiles.toArray(new File[allFiles.size()]);

        return files;
    }

    private String createPattern(String inputString) {
        String OPEN_QUOTE = "\\Q";
        String END_QUOTE = "\\E";
        String dotPattern = OPEN_QUOTE + "." + END_QUOTE;
        String asterixPattern = ".*";
        String inputPattern = null;

        int dotLocn = inputString.indexOf(".");
        inputPattern = inputString.substring(0, dotLocn) + dotPattern
                + inputString.substring(dotLocn + 1, inputString.length());

        String outputPattern = "";
        String holdPattern = inputPattern;
        boolean done = false;
        while (!done) {
            int asterixPos = holdPattern.indexOf("*");

            if (asterixPos < 0) {
                done = true;
            } else {
                outputPattern = outputPattern + holdPattern.substring(0, asterixPos) + asterixPattern;
                holdPattern = holdPattern.substring(asterixPos + 1, holdPattern.length());
            }
        }

        return outputPattern;
    }

}
