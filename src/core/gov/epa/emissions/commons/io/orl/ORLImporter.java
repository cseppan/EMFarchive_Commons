package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetTypeUnit;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.CustomCharSetInputStreamReader;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.Comments;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.DatasetLoader;
import gov.epa.emissions.commons.io.importer.DelimiterIdentifyingFileReader;
import gov.epa.emissions.commons.io.importer.FileVerifier;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.TemporalResolution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

public class ORLImporter {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private DataTable dataTable;

    private DatasetLoader loader;
    
    private Record record;

    public ORLImporter(File folder, String[] filePatterns, Dataset dataset, DatasetTypeUnit formatUnit,
            Datasource datasource) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filePatterns);
        this.file = new File(folder, filePatterns[0]);

        this.dataset = dataset;
        this.formatUnit = formatUnit;
        this.datasource = datasource;
        dataTable = new DataTable(dataset, datasource);
        loader = new DatasetLoader(dataset);
        loader.internalSource(file, dataTable.name(), formatUnit.tableFormat());
    }

    public void run() throws ImporterException {
        importAttributes(file, dataset);
        dataTable.create(formatUnit.tableFormat(), dataset.getId());
        try {
            doImport(file, dataset, dataTable.name(), (FileFormatWithOptionalCols) formatUnit.fileFormat());
        } catch (Exception e) {
            dataTable.drop();
            throw new ImporterException("Filename: " + file.getAbsolutePath() + ", " + e.getMessage());
        }
    }

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat) throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir");
        File headerFile = new File(tempDir, ".header");
        File dataFile = new File(tempDir, ".data");
        
        splitFile(file, headerFile.getAbsolutePath(), dataFile.getAbsolutePath());
        loadDataset(getComments(headerFile), dataset);
        
        String copyString = "COPY " + getFullTableName(table) + " (" + getColNames(fileFormat, dataFile) + ") FROM '" + dataFile.getAbsolutePath()
                + "' WITH CSV QUOTE AS '\"'" ;

        Connection connection = datasource.getConnection();
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.execute(copyString);
        statement.close();
        
        headerFile.delete();
        dataFile.delete();
    }
    
    private void splitFile(File file, String headerFile, String dataFile) throws IOException, InterruptedException {
        String[] cmd = null;

        String headerCmd = "grep \"^#\" " + file.getAbsolutePath() + " > " + headerFile;
        String dataCmd = "grep -v \"^#\" " + file.getAbsolutePath() + " | grep -v \"^[[:space:]]*$\" > " + dataFile;
        
        cmd = new String[] { "sh", "-c", headerCmd + ";" + dataCmd};

        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
    }

    private List<String> getComments(File file) throws Exception {
        BufferedReader fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(file)));
        List<String> commentsList = new ArrayList<String>();
        String line = fileReader.readLine();
        
        while (line != null) {
            commentsList.add(line);
            line = fileReader.readLine();
        }
        
        fileReader.close();

        return commentsList;
    }
    
    private String getFullTableName(String table) {
        return this.datasource.getName() + "." + table;
    }

    private String getColNames(FileFormatWithOptionalCols fileFormat, File dataFile) {
        Column[] cols = fileFormat.cols();
        String colsString = "";

        for (int i = 0; i < record.size(); i++)
            colsString += cols[i].name() + ",";

        return colsString.substring(0, colsString.length() - 1);
    }

    private void loadDataset(List<String> comments, Dataset dataset) {
        dataset.setUnits("short tons/year");
        dataset.setTemporalResolution(TemporalResolution.ANNUAL.getName());
        dataset.setDescription(new Comments(comments).all());
    }

    private void importAttributes(File file, Dataset dataset) throws ImporterException {
        DelimiterIdentifyingFileReader reader = null;
        try {
            // FIXME: move 'minCols' to FileFormat
            reader = new DelimiterIdentifyingFileReader(file, ((FileFormatWithOptionalCols) formatUnit.fileFormat())
                    .minCols().length);
            record = reader.read();
            
            if (!reader.delimiter().equals(","))
                throw new ImporterException("Data file is not delimited by comma.");
            
            reader.close();
        } catch (IOException e) {
            throw new ImporterException(e.getMessage());
        }
        addAttributes(reader.comments(), dataset);
    }

    private void addAttributes(List<String> commentsList, Dataset dataset) throws ImporterException {
        Comments comments = new Comments(commentsList);
        if (!comments.have("ORL"))
            throw new ImporterException("The tag - 'ORL' is mandatory.");

        if (!comments.hasContent("COUNTRY"))
            throw new ImporterException("The tag - 'COUNTRY' is mandatory.");
        // BUG: Country should not be created, but looked up
        // String country = comments.content("COUNTRY");
        // dataset.setCountry(new Country(country));

        if (!comments.hasContent("YEAR"))
            throw new ImporterException("The tag - 'YEAR' is mandatory.");
        String year = comments.content("YEAR");
        dataset.setYear(Integer.parseInt(year));
        setStartStopDateTimes(dataset, Integer.parseInt(year));
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

}
