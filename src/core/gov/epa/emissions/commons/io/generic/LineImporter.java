package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.File;

public class LineImporter implements Importer {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    public LineImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        create(folder, filenames, dataset, datasource, sqlDataTypes, new FixedDataFormatFactory());
    }

    public LineImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        create(folder, filenames, dataset, datasource, sqlDataTypes, factory);
    }
    
    private void create(File folder, String[] filenames, Dataset dataset, Datasource datasource, 
            SqlDataTypes types, DataFormatFactory factory) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filenames);
        this.file = new File(folder, filenames[0]);
        this.dataset = dataset;
        this.datasource = datasource;
        FileFormat fileFormat = new LineFileFormat(types);
        TableFormat tableFormat = factory.tableFormat(fileFormat, types);
        this.formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }
    
    public void run() throws ImporterException {
        DataTable dataTable = new DataTable(dataset, datasource);
        String table = dataTable.name();
        try {
            if(!dataTable.exists(table))
                dataTable.create(formatUnit.tableFormat());
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            dataTable.drop(table);
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        DataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        Reader reader = new LineReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.tableFormat(), dataset);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
    }
}
