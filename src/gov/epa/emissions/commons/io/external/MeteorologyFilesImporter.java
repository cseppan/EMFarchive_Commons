/*
 * Creation on Oct 11, 2005
 * Eclipse Project Name: Commons
 * File Name: MeteorologyFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MeteorologyFilesImporter  extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(MeteorologyFilesImporter.class);

	public MeteorologyFilesImporter(File folder, String filePattern, Dataset dataset) throws ImporterException {
		super(folder, filePattern, dataset);
		importerName = "Meteorology Files Importer";
		log.debug("Default Meteorology Files importer created");
	}

}
