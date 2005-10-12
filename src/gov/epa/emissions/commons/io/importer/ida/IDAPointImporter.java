package gov.epa.emissions.commons.io.importer.ida;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.ImporterException;

public class IDAPointImporter {

	private IDAImporter delegate;

	private SqlDataTypes sqlDataTypes;

	public IDAPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.sqlDataTypes = sqlDataTypes;
		delegate = new IDAImporter(datasource, sqlDataTypes);
	}

	public void run(File file, Dataset dataset) throws ImporterException {
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(file));
			IDAHeaderReader headerReader = new IDAHeaderReader(reader);
			headerReader.read();
			ColumnsMetadata colsMetadata = new IDAPointColumnsMetadata(
					headerReader.polluntants(), sqlDataTypes);

			delegate
					.run(reader, colsMetadata, headerReader.comments(), dataset);

		} catch (Exception e) {
			throw new ImporterException("could not import File - "
					+ file.getAbsolutePath() + " into Dataset - "
					+ dataset.getName());
		}

	}

}
