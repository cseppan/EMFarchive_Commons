/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: ShapeFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShapeFilesImporter extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ShapeFilesImporter.class);

	public ShapeFilesImporter(File folder, String filePattern, Dataset dataset) throws ImporterException {
		super(folder, filePattern, dataset);
		importerName = "Shape Files Importer";
		log.debug("Default Shape Files importer created");
	}

}
