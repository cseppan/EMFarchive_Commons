package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.CommaDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PipeDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.SemiColonDelimitedTokenizer;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SMKReportImporter implements Importer {
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private SMKReportFileFormatFactory factory;

    private String delimiter;

    public SMKReportImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes) throws ImporterException,IOException,Exception {
        this(folder, filenames, dataset, datasource, sqlDataTypes, new FixedDataFormatFactory());
    }
    
    public SMKReportImporter(File folder, String[] filenames, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes, DataFormatFactory dataFormatFactory) throws ImporterException,IOException,Exception {
        new FileVerifier().shouldHaveOneFile(filenames);
        this.file = new File(folder, filenames[0]);
        this.dataset = dataset;
        this.datasource = datasource;
        this.factory = new SMKReportFileFormatFactory(file, sqlDataTypes);
        this.delimiter = factory.getDelimiter();

        FileFormat fileFormat = factory.getFormat();
        TableFormat tableFormat = dataFormatFactory.tableFormat(fileFormat, sqlDataTypes);
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
            dataTable.drop();
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    // FIXME: have to use a delimited identifying reader
    private void doImport(File file, Dataset dataset, String table, TableFormat tableFormat) throws Exception {
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, tableFormat);
        DelimitedFileReader reader = getFileReader();
        List comments = getComments(reader);
        loader.load(reader, dataset, table);
        loadDataset(file, table, formatUnit.tableFormat(), dataset, comments);
    }

    private void loadDataset(File file, String table, TableFormat tableFormat, Dataset dataset, List comments) {
        DatasetLoader loader = new DatasetLoader(dataset);
        loader.internalSource(file, table, tableFormat);
        dataset.setDescription(new Comments(comments).all());
    }

    private List getComments(DelimitedFileReader reader) throws IOException {
        List comments = reader.comments();
        String[] headers = reader.readHeader("[" + delimiter + "]");
        for (int i = 0; i < headers.length; i++) {
            comments.add(headers[i]);
        }

        return comments;
    }

    private DelimitedFileReader getFileReader() throws Exception {
        if (delimiter.equals(";"))
            return new DelimitedFileReader(file, new SemiColonDelimitedTokenizer());
        else if (delimiter.equals("|"))
            return new DelimitedFileReader(file, new PipeDelimitedTokenizer());
        else if (delimiter.equals(","))
            return new DelimitedFileReader(file, new CommaDelimitedTokenizer());
        else {
            throw new ImporterException("could not find delimiter - " + file.getAbsolutePath());
        }
    }

}
