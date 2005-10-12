/*
 * Creation on Oct 3, 2005
 * Eclipse Project Name: Commons
 * File Name: ShapeFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.importer.external;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ExternalFilesImporter extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ExternalFilesImporter.class);

	public ExternalFilesImporter() {
		super();
		importerName = "External Files Importer";
		log.debug("Default External (Other) Files importer created");
	}

}
