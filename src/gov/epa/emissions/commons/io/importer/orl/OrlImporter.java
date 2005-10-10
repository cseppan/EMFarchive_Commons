package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;

import java.io.File;
import java.sql.SQLException;

public class OrlImporter {

    private Datasource datasource;

    private TableColumnsMetadata colsMetadata;

    public OrlImporter(Datasource datasource, ColumnsMetadata cols, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        colsMetadata = new TableColumnsMetadata(cols, sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        String table = table(dataset.getName());

        try {
            createTable(table, datasource, colsMetadata);
            doImport(file, dataset, table, colsMetadata);
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        } finally {
            // TODO: drop the table
        }
    }

    private void doImport(File file, Dataset dataset, String table, ColumnsMetadata colsMetadata) throws Exception {
        DataLoader loader = new DataLoader(datasource, colsMetadata);
        Reader reader = new DelimitedFileReader(file);

        loader.load(reader, dataset, table);
    }

    private String table(String datasetName) {
        return datasetName.trim().replaceAll(" ", "_");
    }

    private void createTable(String table, Datasource datasource, ColumnsMetadata colsMetadata) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

}
