package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.File;

public class LineImporter implements Importer {

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;
    
    private HelpImporter delegate;

    public LineImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        FileFormat fileFormat = new LineFileFormat(sqlDataTypes);
        TableFormat tableFormat = new LineTableFormat(fileFormat, sqlDataTypes);
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
        LineLoader loader = new LineLoader(datasource, tableFormat);
        Reader reader = new LineReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.fileFormat(), dataset);
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
    }
}