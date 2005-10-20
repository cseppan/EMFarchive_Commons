/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: AbstractExternalFilesImporter.java
 * Author: Conrad F. D'Cruz
 */
/**
 * 
 */

package gov.epa.emissions.commons.io.importer.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.ExternalSource;
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
public abstract class AbstractExternalFilesImporter implements Importer {
    private static Log log = LogFactory.getLog(AbstractExternalFilesImporter.class);
    protected String importerName;

	/**
	 * 
	 */
	public AbstractExternalFilesImporter() {
		super();
		importerName = "Abstract External Files Importer";
		log.debug("Default AbstractExternal Files importer created");
	}

	public AbstractExternalFilesImporter(String name) {
		super();
		importerName = name;
		log.debug("Abstract External Files importer created: " + name);
	}
	
	/* (non-Javadoc)
	 * @see gov.epa.emissions.commons.io.importer.Importer#run(java.io.File[], gov.epa.emissions.commons.io.Dataset, boolean)
	 */
	public void run(File[] files, Dataset dataset, boolean overwrite)
			throws Exception {

        	log.debug("updating non-ORL dataset");
        	updateExternalDataset(dataset,files);
        	log.debug("completed updating non-ORL dataset");
        }	

	private void updateExternalDataset(Dataset dataset, File[] files) {
		log.debug("Values: " + dataset.getName() + " Total number of files to update= " + files.length);
		ExternalSource extSrc=null;
		
    	for (int i=0; i<files.length;i++){
    		extSrc = new ExternalSource(files[i].getAbsolutePath());
    		extSrc.setListindex(i);
    		dataset.addExternalSource(extSrc);
    	}
    	log.debug("List contains " + dataset.getExternalSources().length + " files to update");
    	
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
        log.debug("Min files for " + datasetType.getName()+": " + minFiles);
        allFiles = extractFileNames(path, fileName);
        log.debug("Number of files in directory: " + allFiles.length);
        if (allFiles.length < minFiles){
        	throw new Exception(importerName + " importer requires " + minFiles + " files");
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
			Pattern pat = Pattern.compile(createPattern(fileName));
			log.debug("PATTERN to match files: " + fileName);
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
	
	private String createPattern(String inputString) {
		System.out.println("INPUT STRING= " + inputString);
		String OPEN_QUOTE = "\\Q";
		String END_QUOTE = "\\E";
		String dotPattern = OPEN_QUOTE + "." + END_QUOTE;
		String asterixPattern=".*";
		String inputPattern = null;
		
		int dotLocn = inputString.indexOf(".");
		inputPattern = inputString.substring(0, dotLocn) + dotPattern
				+ inputString.substring(dotLocn + 1, inputString.length());

		String outputPattern="";
		String holdPattern = inputPattern;
		boolean done=false;
		while (!done){
			int asterixPos = holdPattern.indexOf("*");
			
			if (asterixPos <0) {
				done =true;
			}else{
				outputPattern=outputPattern + holdPattern.substring(0,asterixPos) + asterixPattern;
				holdPattern=holdPattern.substring(asterixPos+1, holdPattern.length());
			}
		}

		return outputPattern;
	}

}
