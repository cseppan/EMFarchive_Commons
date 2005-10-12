/*
 * Creation on Oct 11, 2005
 * Eclipse Project Name: Commons
 * File Name: MeteorologyFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.importer.meteorology;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gov.epa.emissions.commons.io.importer.external.AbstractExternalFilesImporter;

public class MeteorologyFilesImporter  extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(MeteorologyFilesImporter.class);

	public MeteorologyFilesImporter() {
		super();
		importerName = "Meteorology Files Importer";
		log.debug("Default Meteorology Files importer created");
	}

}
