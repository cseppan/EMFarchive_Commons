package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.Record;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

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

	public void run(BufferedReader reader,FileFormat colsMetadata , List comments, Dataset dataset) throws Exception {
		String table = table(dataset.getName());
		try
		{
			doImport(reader,colsMetadata,comments, dataset, table);
		} catch (Exception e) {
			throw e;
		} finally {
			// TODO: drop the table
		}
	}

	private void doImport(BufferedReader reader,FileFormat colsMetadata , List comments, Dataset dataset, String table)
			throws Exception {
		
		TableFormat tableColMetadata = new TableFormat(
				colsMetadata, sqlDataTypes);

		createTable(table, datasource, tableColMetadata);

		Reader idaReader = new IDAFileReader(reader, colsMetadata, comments);
		FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableColMetadata);

		Record record = idaReader.read();
		
		List headerComments = idaReader.comments();
		checkHeaderTags(headerComments);
		
		loader.insertRow(record,dataset,table);
		loader.load(idaReader, dataset, table);
	}

	private void checkHeaderTags(List headerComments) throws Exception {
		checkTag("#IDA",headerComments);
		checkTag("#COUNTRY",headerComments);
		checkTag("#YEAR",headerComments);
		checkTag("#DATA","#POLID",headerComments);
	}

	private void checkTag(String tag1, String tag2, List headerComments) throws Exception {
		for(int i=0;i<headerComments.size();i++){
			String comment = (String) headerComments.get(i);
			if(comment.trim().startsWith(tag1) || comment.trim().startsWith(tag2))
				return;
		}
		throw new Exception("Could not find tag '"+tag1 +"' or '"+tag2);
		
	}

	private void checkTag(String tag,List headerComents) throws Exception {
		for(int i=0;i<headerComents.size();i++){
			String comment = (String) headerComents.get(i);
			if(comment.trim().startsWith(tag))
				return;
		}
		throw new Exception("Could not find tag '"+tag +"'");
	}

	private String table(String datasetName) {
		return datasetName.trim().replaceAll(" ", "_");
	}

	private void createTable(String table, Datasource datasource,
			FileFormat colsMetadata) throws SQLException {
		TableDefinition tableDefinition = datasource.tableDefinition();
		tableDefinition.createTable(table, colsMetadata.cols());
	}


}
