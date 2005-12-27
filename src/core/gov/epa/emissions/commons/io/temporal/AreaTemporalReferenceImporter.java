package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class AreaTemporalReferenceImporter implements Importer {
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private DatasetTypeUnit unit;

    /**
     * Expects table 'AREA_SOURCE' to be available in Datasource
     */
    public AreaTemporalReferenceImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.file = file;
        this.dataset = dataset;
        this.datasource = datasource;
        AreaTemporalReferenceFileFormat fileFormat = new AreaTemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        unit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    public void run() throws ImporterException {
        try {
            doImport(file, dataset, "AREA_SOURCE", unit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException(e.getMessage() + " Filename: " + file.getAbsolutePath() + "\n");
        }
    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        DataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        Reader reader = new TemporalReferenceReader(fileReader);

        loader.load(reader, dataset, table);
        loadDataset(file, table, tableFormat, reader, dataset);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Reader reader, Dataset dataset) {
        // TODO: other properties ?
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
        Comments comments = new Comments(reader.comments());
        dataset.setDescription(comments.all());
    }

}
