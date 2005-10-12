package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.io.BufferedReader;
import java.sql.SQLException;
import java.util.List;

public class IDAImporter {

	private Datasource datasource;

	private SqlDataTypes sqlDataTypes;

	public IDAImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.sqlDataTypes = sqlDataTypes;
		this.datasource = datasource;
	}

	public void run(BufferedReader reader,ColumnsMetadata colsMetadata , List comments, Dataset dataset) throws Exception {
		String table = table(dataset.getName());
		try
		{
			doImport(reader,colsMetadata,comments, dataset, table);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
			
		} finally {
			// TODO: drop the table
		}
	}

	private void doImport(BufferedReader reader,ColumnsMetadata colsMetadata , List comments, Dataset dataset, String table)
			throws Exception {
		
		TableColumnsMetadata tableColMetadata = new TableColumnsMetadata(
				colsMetadata, sqlDataTypes);

		createTable(table, datasource, tableColMetadata);

		Reader idaReader = new IDAFileReader(reader, colsMetadata, comments);
		DataLoader loader = new DataLoader(datasource, tableColMetadata);

		loader.load(idaReader, dataset, table);
	}

	private String table(String datasetName) {
		return datasetName.trim().replaceAll(" ", "_");
	}

	private void createTable(String table, Datasource datasource,
			ColumnsMetadata colsMetadata) throws SQLException {
		TableDefinition tableDefinition = datasource.tableDefinition();
		tableDefinition.createTable(table, colsMetadata.colNames(),
				colsMetadata.colTypes());
	}

}
