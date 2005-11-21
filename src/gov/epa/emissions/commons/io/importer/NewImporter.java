package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;

import java.io.File;

public interface NewImporter {

    /**
     * This method checks specific preconditions for the different types of
     * importers.
     * 
     * For eg.
     * 
     * External files will be specified by regular expression patterns.
     * 
     * Shape files need a minimum of 3 input files Meteorology files will take
     * in a minimum of 1 input file
     * 
     * @throws Exception
     */
    void preCondition(File folder, String filePattern) throws Exception;

    /**
     * This method will put the files into the dataset and database, overwriting
     * existing tables if authorized.
     */
    // TODO: have a separate method for overwrite & non-overwrite
    void run(Dataset dataset) throws Exception;
    
    InternalSource[] internalSources();

}
