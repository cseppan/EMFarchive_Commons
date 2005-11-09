/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: ShapeFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.io.DatasetType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ShapeFilesImporter extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ShapeFilesImporter.class);

	public ShapeFilesImporter(DatasetType datasetType) {
		super(datasetType);
		importerName = "Shape Files Importer";
		log.debug("Default Shape Files importer created");
	}

}
