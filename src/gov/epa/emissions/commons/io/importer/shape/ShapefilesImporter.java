/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: ShapefilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.importer.shape;

import java.io.File;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.external.ExternalfilesImporter;

public class ShapefilesImporter extends ExternalfilesImporter {

	public ShapefilesImporter() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * Check the preconditions for Shapefiles.
	 * 
	 */
	public void preCondition() throws Exception {
		
	
	}

	public void run(File[] files, Dataset dataset, boolean overwrite) throws Exception {
		super.run(files, dataset, overwrite);
	}


}
