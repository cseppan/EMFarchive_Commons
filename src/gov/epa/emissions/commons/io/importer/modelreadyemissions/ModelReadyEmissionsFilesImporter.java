/*
 * Creation on Oct 11, 2005
 * Eclipse Project Name: Commons
 * File Name: ModelReadyEmissionsFilesImporter.java
 * Author: Conrad F. D'Cruz
 */

package gov.epa.emissions.commons.io.importer.modelreadyemissions;

import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.importer.external.AbstractExternalFilesImporter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ModelReadyEmissionsFilesImporter  extends AbstractExternalFilesImporter {
    private static Log log = LogFactory.getLog(ModelReadyEmissionsFilesImporter.class);

	public ModelReadyEmissionsFilesImporter(DatasetType datasetType) {
		super(datasetType);
		importerName = "Model Ready Emissions Files Importer";
		log.debug("Default ModelReadyEmissions Files importer created");
	}


}
