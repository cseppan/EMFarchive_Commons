package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class InventoryTableImporter {
    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;
    
    private HelpImporter delegate;

    public InventoryTableImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        FileFormat fileFormat = new InventoryTableFileFormat(sqlDataTypes, 1);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        this.delegate = new HelpImporter();
    }

    public void preCondition(File folder, String filePattern) {
        File file = new File(folder, filePattern);
        try{
            delegate.validateFile(file);
        }catch (ImporterException e){
            e.printStackTrace();
        }
        this.file = file;
    }

    public void run(Dataset dataset) throws ImporterException {
        String table = delegate.tableName(dataset.getName());
        delegate.createTable(table, datasource, formatUnit.tableFormat(), dataset.getName());

        try {
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            delegate.dropTable(table, datasource);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }

    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        BufferedReader reader = new BufferedReader(new FileReader(file.getAbsolutePath()));
        //FIXME: Due to irregularity in inventory data names (1st column in input data file),
        //some extra chars may be extracted into 2nd column (CAS number).
        Reader fileReader = new DataReader(reader, new InventoryTableParser(formatUnit.fileFormat()));

        loader.load(fileReader, dataset, table);
        loadDataset(file, table, formatUnit.fileFormat(), dataset);
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
    }
}