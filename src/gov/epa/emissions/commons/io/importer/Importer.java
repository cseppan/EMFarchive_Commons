package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Dataset;

import java.io.File;

public interface Importer {
	
	/**
	 * This method checks specific preconditions for the
	 * different types of importers.
	 * 
	 * For eg.
	 * 
	 * External files will be specified by regular expression
	 * patterns.
	 * 
	 * Shape files need a minimum of 3 input files
	 * Meteorology files will take in a minimum of 1 input file
	 * 
	 * @throws Exception
	 */
	public void preCondition(String fileName) throws Exception;
	
    /**
     * This method will put the files into the dataset and database, overwriting
     * existing tables if authorized.
     */
    // TODO: have a separate method for overwrite & non-overwrite
    public void run(File[] files, Dataset dataset, boolean overwrite) throws Exception;

}
