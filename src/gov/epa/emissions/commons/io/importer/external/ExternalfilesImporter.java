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

import java.io.File;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;

/**
 * @author Conrad F. D'Cruz
 *
 */
public class ExternalfilesImporter implements Importer {

	/**
	 * 
	 */
	public ExternalfilesImporter() {
		super();
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see gov.epa.emissions.commons.io.importer.Importer#preCondition()
	 */
	public void preCondition(String fileName) throws Exception {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see gov.epa.emissions.commons.io.importer.Importer#run(java.io.File[], gov.epa.emissions.commons.io.Dataset, boolean)
	 */
	public void run(File[] files, Dataset dataset, boolean overwrite)
			throws Exception {
		// TODO Auto-generated method stub

	}

}
