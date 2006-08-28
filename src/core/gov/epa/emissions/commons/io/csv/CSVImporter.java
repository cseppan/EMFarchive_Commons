package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetTypeUnit;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.reference.CSVFileFormat;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CSVImporter implements Importer {
    
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private CSVFileReader reader;

    private SqlDataTypes sqlDataTypes;

    public CSVImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        this(folder, filenames, dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public CSVImporter(File folder, String[] filenames, Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory dataFormatFactory) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filenames);
        this.file = new File(folder, filenames[0]);
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.sqlDataTypes = sqlDataTypes;

        reader = new CSVFileReader(file);
        FileFormat fileFormat = fileFormat(reader);
        TableFormat tableFormat = dataFormatFactory.tableFormat(fileFormat, sqlDataTypes);
        formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    public void run() throws ImporterException {
        DataTable dataTable = new DataTable(dataset, datasource);
        String table = dataTable.name();

        try {
            if (!dataTable.exists(table))
                dataTable.create(formatUnit.tableFormat());
            doImport(file, dataset, table, formatUnit.tableFormat());
        } catch (Exception e) {
            dataTable.drop();
            throw new ImporterException(e.getMessage());
        } finally {
            close(reader);
        }
    }

    private void close(Reader reader) throws ImporterException {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
               throw new ImporterException(e.getMessage());
            }
        }
    }

    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        List comments = getComments(reader);
        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.tableFormat(), dataset, comments);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset, List comments) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
        if (comments != null)
            dataset.setDescription(new Comments(comments).all());
    }

    private List getComments(CSVFileReader reader) {
        return reader.getHeader();
    }

    private CSVFileFormat fileFormat(CSVFileReader reader) {
        return new CSVFileFormat(sqlDataTypes, reader.getCols());
    }

}
