/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: ExternalfilesImporter.java
 * Author: Conrad F. D'Cruz
 */
/**
 * 
 */

package gov.epa.emissions.commons.io.importer.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Conrad F. D'Cruz
 *
 */
public class ExternalfilesImporter implements Importer {
    private static Log log = LogFactory.getLog(ExternalfilesImporter.class);


	/**
	 * 
	 */
	public ExternalfilesImporter() {
		super();
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see gov.epa.emissions.commons.io.importer.Importer#run(java.io.File[], gov.epa.emissions.commons.io.Dataset, boolean)
	 */
	public void run(File[] files, Dataset dataset, boolean overwrite)
			throws Exception {
		// TODO Auto-generated method stub

	}

	public File[] preCondition(String folderPath, String fileName) throws Exception {
        File path = validatePath(folderPath);
        File file = validateFile(path, fileName);
        File[] allFiles = null;
        
        
        
        return allFiles;
	}

	public File validatePath(String folderPath) throws Exception {
        log.debug("check if folder exists " + folderPath);
        File file = new File(folderPath);

        if (!file.exists() || !file.isDirectory()) {
            log.error("Folder " + folderPath + " does not exist");
            throw new Exception("Folder does not exist");
        }
        log.debug("check if folder exists " + folderPath);
        return file;
	}

	public File validateFile(File path, String fileName) throws Exception {
        log.debug("check if file exists " + fileName);
        File file = new File(path, fileName);

        if (!file.exists() || !file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new Exception("File not found");
        }
        log.debug("check if file exists " + fileName);

        return file;
	}

}
