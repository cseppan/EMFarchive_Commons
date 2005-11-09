/*
 * Creation on Oct 11, 2005
 * Eclipse Project Name: Commons
 * File Name: MeteorologyFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.external;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gov.epa.emissions.commons.io.DatasetType;

public class MeteorologyFilesImporter  extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(MeteorologyFilesImporter.class);

	public MeteorologyFilesImporter(DatasetType datasetType) {
		super(datasetType);
		importerName = "Meteorology Files Importer";
		log.debug("Default Meteorology Files importer created");
	}

}
