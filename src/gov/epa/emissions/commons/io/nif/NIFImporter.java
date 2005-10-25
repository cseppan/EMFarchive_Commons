package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;

public class NIFImporter {

	private Datasource datasource;

	private SqlDataTypes sqlDataTypes;

	public NIFImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.datasource = datasource;
		this.sqlDataTypes = sqlDataTypes;
	}

	public void run(Dataset dataset) {
		InternalSource[] sources = dataset.getInternalSources();
		for (int i = 0; i < sources.length; i++) {
		
		}
	}

}
