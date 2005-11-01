package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.io.File;
import java.sql.SQLException;

public class NIFImporter {

	private Datasource datasource;

	private SqlDataTypes sqlDataTypes;

	public NIFImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
		this.datasource = datasource;
		this.sqlDataTypes = sqlDataTypes;
	}

	public void run(File file, String tableName, DatasetTypeUnit unit, Dataset dataset) throws ImporterException {
        try {
            createTable(tableName,unit.tableFormat());
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + dataset.getName(), e);
        }
        try {
            doImport(file, dataset, tableName, unit.fileFormat(),unit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
        
	}

	private void doImport(File file, Dataset dataset, String tableName, FileFormat fileFormat, TableFormat tableFormat) throws ImporterException {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        Reader reader =  null;
        loader.load(reader, dataset, tableName);
        //TODO: load dataset
    }

    private void createTable(String tableName, TableFormat tableFormat) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(tableName, tableFormat.cols());
    }

}
