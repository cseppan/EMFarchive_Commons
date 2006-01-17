package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FilesFromPattern {

    private File[] files;

    public FilesFromPattern(File folder, String[] filePatterns,Dataset dataset) throws ImporterException{
        DatasetType datasetType = dataset.getDatasetType();
        if (filePatterns.length > 1) {
            throw new ImporterException("Only one file pattern or filename is allowed, but "+filePatterns.length+ " are specified");
        }
        if (filePatterns[0].length() == 0) {
            throw new ImporterException("No file pattern or filename is specified");
        }

        int minFiles = datasetType.getMinfiles();
        files = extractFileNames(folder, filePatterns[0]);
        if (files.length < minFiles) {
            throw new ImporterException(datasetType.getName()+ " importer requires " + minFiles + " files");
        } 
    }

    public File[] files(){
        return files;
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
