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
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.importer.Importer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Conrad F. D'Cruz
 *
 */
public class ExternalfilesImporter implements Importer {
    private static Log log = LogFactory.getLog(ExternalfilesImporter.class);
    protected String importerName;

	/**
	 * 
	 */
	public ExternalfilesImporter() {
		super();
		importerName = "External files";
	}

	/* (non-Javadoc)
	 * @see gov.epa.emissions.commons.io.importer.Importer#run(java.io.File[], gov.epa.emissions.commons.io.Dataset, boolean)
	 */
	public void run(File[] files, Dataset dataset, boolean overwrite)
			throws Exception {
		// TODO Auto-generated method stub

	}

	public File validateFile(File path, String fileName) throws Exception {
        log.debug("begin check if file exists " + fileName);
        File file = new File(path, fileName);

        if (!file.exists() || !file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new Exception("File not found");
        }
        log.debug("end check if file exists " + fileName);

        return file;
	}

	public File[] preCondition(File path, String fileName, DatasetType datasetType) throws Exception {
        File[] allFiles = null;
        
        int minFiles = datasetType.getMinfiles();

        allFiles = extractFileNames(path, fileName);
        if (allFiles.length < minFiles){
        	throw new Exception(importerName + " importer requires " + minFiles + " files.  Only " + allFiles.length + " available");
        }
        
        
        return allFiles;
	}

	private File[] extractFileNames(File path, String fileName) throws Exception {
		List allFiles = new ArrayList();
		
		File[] files = null;
		
		if (fileName.indexOf("*")>=0){
			String[] allFilesInFolder = path.list();
			for (int i=0; i<allFilesInFolder.length;i++){
				log.debug("file: " + allFilesInFolder[i]);
			}
			Pattern pat = Pattern.compile(fileName);
			log.debug("PATTERN: " + fileName);
			for (int i = 0; i < allFilesInFolder.length; i++) {
				String ipFile = allFilesInFolder[i];
				log.debug("File #" + i + "/" + allFilesInFolder.length + " FileName= " +ipFile);
				Matcher m = pat.matcher(ipFile);
				if ( m.matches()){
					allFiles.add(validateFile(path,ipFile));
		        }				
			}
		}else{
			allFiles.add(validateFile(path,fileName));
		}
		log.debug("objects in list: " + allFiles.size());
		files = (File[])allFiles.toArray(new File[allFiles.size()]);
		log.debug("Selected files in array: " + files.length);

		return files;
	}


}
