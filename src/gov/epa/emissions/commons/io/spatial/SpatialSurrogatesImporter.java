package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.WhitespaceDelimitedFileReader;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

import java.io.File;
import java.util.List;

public class SpatialSurrogatesImporter implements Importer {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private HelpImporter delegate;

    public SpatialSurrogatesImporter(File file, Dataset dataset, Datasource datasource, 
            SqlDataTypes sqlDataTypes) throws ImporterException {
        
        FileFormat fileFormat = new SpatialSurrogatesFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        this.delegate = new HelpImporter();
        this.dataset = dataset;
        this.datasource = datasource;
        this.formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        
        setup(file);
    }

    private void setup(File file) throws ImporterException {
        delegate.validateFile(file);
        this.file = file;
    }

    public void run() throws ImporterException {
        String table = delegate.tableName(dataset.getName());
        
        try{
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    // FIXME: have to use a delimited identifying reader
    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        Reader reader = new WhitespaceDelimitedFileReader(file);

        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.fileFormat(), dataset, reader.comments());
    }

    private void loadDataset(File file, String table, FileFormat fileFormat, Dataset dataset, List comments) {
        delegate.setInternalSource(file, table, fileFormat, dataset);
        dataset.setDescription(delegate.descriptions(comments));
    }
}
