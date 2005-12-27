package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetLoader;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter_REMOVE_ME;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.File;

public class LineImporter implements Importer {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private HelpImporter_REMOVE_ME delegate;

    public LineImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.delegate = new HelpImporter_REMOVE_ME();
        this.file = file;
        this.dataset = dataset;
        this.datasource = datasource;
        FileFormat fileFormat = new LineFileFormat(sqlDataTypes);
        TableFormat tableFormat = new LineTableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    public void run() throws ImporterException {
        String table = delegate.tableName(dataset.getName());
        try {
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }

    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        LineLoader loader = new LineLoader(datasource, tableFormat);
        Reader reader = new LineReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.tableFormat(), dataset);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
    }
}
