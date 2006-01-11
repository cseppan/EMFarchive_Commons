package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingTokenizer;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;
import java.util.List;

public class CEMHourSpecInventoryImporter implements Importer {
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    public CEMHourSpecInventoryImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        this(folder, filenames, dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }
    
    public CEMHourSpecInventoryImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filenames);
        this.file = new File(folder, filenames[0]);
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();

        FileFormat fileFormat = new CEMHourSpecInventFileFormat(sqlDataTypes);
        TableFormat tableFormat = factory.tableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    public void run() throws ImporterException {
        DataTable dataTable = new DataTable(dataset, datasource);
        String table = dataTable.name();

        try {
            if(!dataTable.exists(table))
                dataTable.create(formatUnit.tableFormat());
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    // FIXME: have to use a delimited identifying reader
    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        DelimitedFileReader reader = new DelimitedFileReader(file, 
                new DelimiterIdentifyingTokenizer(formatUnit.fileFormat().cols().length));
        String[] header = reader.readHeader(1);
        List comments = reader.comments();

        loader.load(reader, dataset, table);
        comments.add(header[0]);
        loadDataset(file, table, formatUnit.tableFormat(), dataset, comments);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset, List comments) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
        dataset.setDescription(new Comments(comments).all());
    }

}
