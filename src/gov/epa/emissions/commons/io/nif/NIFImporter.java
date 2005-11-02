package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedWidthParser;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class NIFImporter {

    private Datasource datasource;

    private List tableNames;

    public NIFImporter(Datasource datasource) {
        this.datasource = datasource;
        this.tableNames = new ArrayList();
    }

    public void run(InternalSource internalSource, FormatUnit unit, Dataset dataset) throws ImporterException {
        try {
            String tableName = internalSource.getTable();
            createTable(tableName, unit.tableFormat());
            tableNames.add(tableName);
            doImport(internalSource.getSource(), dataset, tableName, unit.fileFormat(), unit.tableFormat());
        } catch (Exception e) {
            dropTables(tableNames);
            throw new ImporterException("could not import File - " + internalSource + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void dropTables(List tableNames) throws ImporterException {
        String tableName = null;
        for (int i = 0; i < tableNames.size(); i++) {
            try {
                tableName = (String) tableNames.get(i);
                datasource.tableDefinition().deleteTable(tableName);
            } catch (SQLException e) {
                throw new ImporterException("Could not drop table '" + tableName + "'\n" + e.getMessage());
            }
        }
    }

    private void createTable(String tableName, TableFormat tableFormat) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(tableName, tableFormat.cols());
    }

    private void doImport(String fileName, Dataset dataset, String tableName, FileFormat fileFormat,
            TableFormat tableFormat) throws ImporterException, IOException {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        Reader fileReader = new DataReader(reader, new FixedWidthParser(fileFormat));
        loader.load(fileReader, dataset, tableName);
        reader.close();
        // TODO: load dataset
    }

}
