package gov.epa.emissions.commons.io.reference;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.CommaDelimitedTokenizer;
import gov.epa.emissions.commons.io.importer.DelimitedFileReader;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReferenceCSVFileImporter implements Importer {

    private Datasource datasource;

    private File file;

    private SqlDataTypes sqlDataTypes;

    private String tableName;

    private Reader reader;

    private FileFormat fileFormat;

    public ReferenceCSVFileImporter(File file, String tableName, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        this.file = file;
        this.tableName = tableName;
        this.datasource = datasource;
        this.sqlDataTypes = sqlDataTypes;
        this.file = file;

        try {
            reader = new DelimitedFileReader(file, new CommaDelimitedTokenizer());
        } catch (FileNotFoundException e) {
            throw new ImporterException("File not found: " + file.getAbsolutePath() + "\n" + e.getMessage());
        }
        this.fileFormat = fileFormat(reader);
    }

    public void run() throws ImporterException {
        createTable(tableName, datasource, fileFormat);
        try {
            doImport();
        } catch (Exception e) {
            dropTable(tableName);
            throw new ImporterException("Could not import file: " + file.getAbsolutePath() + "\n" + e.getMessage());
        }
    }

    private void dropTable(String tableName) throws ImporterException {
        try {
            datasource.tableDefinition().dropTable(tableName);
        } catch (SQLException e) {
            throw new ImporterException("could not drop table '"+tableName+"'");
        }        
    }

    private void doImport() throws IOException, SQLException, ImporterException {
        Record record = reader.read();
        while (!record.isEnd()) {
            datasource.dataModifier().insertRow(tableName, data(record.getTokens(), fileFormat));
            record = reader.read();
        }
    }

    private String[] data(String[] tokens, FileFormat fileFormat) throws ImporterException {
        int noEmptyTokens = fileFormat.cols().length - tokens.length;
        if (noEmptyTokens < 0) {
            throw new ImporterException("Number of columns in table '" + tableName
                    + "' is less than number of tokens\nLine number -" + reader.lineNumber() + ", Line-"
                    + reader.line());
        }
        List d = new ArrayList(Arrays.asList(tokens));
        for (int i = 0; i < noEmptyTokens; i++) {
            d.add("");
        }
        return (String[]) d.toArray(new String[0]);
    }

    //FIXME: use DataTable to create instead
    private void createTable(String tableName, Datasource datasource, FileFormat fileFormat) throws ImporterException {
        try {
            datasource.tableDefinition().createTable(tableName, fileFormat.cols());
        } catch (SQLException e) {
            throw new ImporterException("Could not create table -" + tableName + "\n" + e.getMessage());
        }
    }

    private ReferenceCSVFileFormat fileFormat(Reader reader) throws ImporterException {
        try {
            Record record = reader.read();
            String[] tokens = record.getTokens();
            return new ReferenceCSVFileFormat(sqlDataTypes, tokens);
        } catch (IOException e) {
            throw new ImporterException(e.getMessage());
        }
    }

}