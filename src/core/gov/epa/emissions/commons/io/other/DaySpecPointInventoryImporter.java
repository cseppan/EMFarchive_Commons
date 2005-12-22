package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.DataReader;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedWidthParser;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.MassagedFixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

public class DaySpecPointInventoryImporter implements Importer {
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private HelpImporter delegate;

    public DaySpecPointInventoryImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        this.delegate = new HelpImporter();

        setup(file);
        this.dataset = dataset;
        this.datasource = datasource;
        FileFormat fileFormat = new DaySpecPointInventoryFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    private void setup(File file) throws ImporterException {
        delegate.validateFile(file);
        this.file = file;
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

    // FIXME: have to use a delimited identifying reader
    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        MassagedFixedColumnsDataLoader loader = new MassagedFixedColumnsDataLoader(datasource, tableFormat);
        BufferedReader reader = new BufferedReader(new FileReader(file.getAbsolutePath()));
        Reader fileReader = new DataReader(reader, new FixedWidthParser(formatUnit.fileFormat()));

        loader.load(fileReader, dataset, table);
        loadDataset(file, table, formatUnit.tableFormat(), dataset, fileReader.comments());
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset, List comments) {
        delegate.setInternalSource(file, table, tableFormat, dataset);
        dataset.setDescription(delegate.descriptions(comments));
    }
}
