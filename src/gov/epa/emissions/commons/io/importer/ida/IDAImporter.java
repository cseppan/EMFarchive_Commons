package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.List;

public class IDAImporter {

	private Datasource datasource;

	private SqlDataTypes sqlDataTypes;

	public IDAImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.sqlDataTypes = sqlDataTypes; 
		this.datasource = datasource;
	}

	public void run(File file, Dataset dataset) throws ImporterException {
		String table = table(dataset.getName());
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			IDAHeaderReader headerReader = new IDAHeaderReader(reader);
			headerReader.read();
			String[] pollutantCols= headerReader.polluntants();
			ColumnsMetadata colsMetadata = new IDAAreaColumnsMetadata(pollutantCols,sqlDataTypes);
			List comments = headerReader.comments();
			doImport(reader, colsMetadata,comments,dataset, table);
		} catch (Exception e) {
			e.printStackTrace();
			throw new ImporterException("could not import File - "
					+ file.getAbsolutePath() + " into Dataset - "
					+ dataset.getName());
		} finally {
			// TODO: drop the table
		}
	}

	private void doImport(BufferedReader reader, ColumnsMetadata colsMetadata, List comments, Dataset dataset, String table) throws Exception {
		Reader idaReader = new IDAFileReader(reader,colsMetadata,comments);
		TableColumnsMetadata tableColMetadata = new TableColumnsMetadata(colsMetadata,sqlDataTypes);
		createTable(table, datasource, tableColMetadata);
		DataLoader loader = new DataLoader(datasource, tableColMetadata);

		loader.load(idaReader, dataset, table);
	}

	private String table(String datasetName) {
		return datasetName.trim().replaceAll(" ", "_");
	}

	private void createTable(String table, Datasource datasource,
			ColumnsMetadata colsMetadata) throws SQLException {
		TableDefinition tableDefinition = datasource.tableDefinition();
		tableDefinition.createTable(table, colsMetadata
				.colNames(), colsMetadata.colTypes());
	}

}
