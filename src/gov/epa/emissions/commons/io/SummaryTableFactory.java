package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonpointNonRoadSummary;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadSummary;
import gov.epa.emissions.commons.io.nif.point.NIFPointSummary;

public class SummaryTableFactory {

	private DbServer dbServer;

	public SummaryTableFactory(DbServer dbServer) {
		this.dbServer = dbServer;
	}

	public SummaryTable create(Dataset dataset) {
		SummaryTable importer = null;
		DatasetType datasetType = dataset.getDatasetType();
		String prefix = datasetType.getName().toLowerCase();
		if (prefix.indexOf("nif3") >= 0)
			return nifSummaryTable(dbServer, dataset);
		/*FIXME: 
		 * if (prefix.indexOf("ida") >= 0) return idaSummaryTable(dbServer, dataset * ); 
		 * if (prefix.indexOf("orl") >= 0) return orlSummaryTable(dbServer, dataset);
		 */
		return importer;
	}

	// FIXME: find a better way to create the importer than comparing string
	private SummaryTable nifSummaryTable(DbServer dbServer, Dataset dataset) {
		Datasource emissions = dbServer.getEmissionsDatasource();
		Datasource reference = dbServer.getReferenceDatasource();

		DatasetType datasetType = dataset.getDatasetType();
		String name = datasetType.getName().toLowerCase();
		if (name.indexOf("nonpoint") >= 0)
			return new NIFNonpointNonRoadSummary(emissions,reference,dataset);
		
		if (name.indexOf("nonroad") >= 0)
			return new NIFNonpointNonRoadSummary(emissions,reference,dataset);
		
		if (name.indexOf("point") >= 0)
			return new NIFPointSummary(emissions,reference,dataset);
		
		if (name.indexOf("onroad") >= 0)
			return new NIFOnRoadSummary(emissions,reference,dataset);
		
		throw new RuntimeException("Dataset Type - " + name + " unsupported");
	}


}
