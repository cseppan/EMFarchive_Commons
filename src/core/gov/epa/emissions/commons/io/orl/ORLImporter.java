package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetTypeUnit;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.CustomCharSetInputStreamReader;
import gov.epa.emissions.commons.io.CustomCharSetOutputStreamWriter;
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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

public class ORLImporter {

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit formatUnit;

    private DataTable dataTable;

    private DatasetLoader loader;

    private Record record;

    private boolean windowsOS = false;

    public ORLImporter(File folder, String[] filePatterns, Dataset dataset, DatasetTypeUnit formatUnit,
            Datasource datasource) throws ImporterException {
        new FileVerifier().shouldHaveOneFile(filePatterns);
        this.file = new File(folder, filePatterns[0]);
        this.dataset = dataset;
        this.formatUnit = formatUnit;
        this.datasource = datasource;
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            windowsOS = true;

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
            e.printStackTrace();
            throw new ImporterException("Filename: " + file.getAbsolutePath() + "; Exception: " + e.getMessage());
        }
    }

    /*
     * @return String a name that is save to use as a file name
     */
    public String getNameForFile(String name) {
        String fileName = new String(name);
        for (int i = 0; i < fileName.length(); i++) {
            if (!Character.isLetterOrDigit(fileName.charAt(i))) {
                fileName = fileName.replace(fileName.charAt(i), '_');
            }
        }
        return fileName;
    }

    private void doImport(File file, Dataset dataset, String table, FileFormatWithOptionalCols fileFormat)
            throws Exception {
        File headerFile = null;
        File dataFile = null;
        Connection connection = null;
        try {
            String tempDir = System.getProperty("IMPORT_EXPORT_TEMP_DIR");

            if (tempDir == null || tempDir.isEmpty())
                tempDir = System.getProperty("java.io.tmpdir");

            File tempDirFile = new File(tempDir);

            if (!(tempDirFile.exists() && tempDirFile.isDirectory() && tempDirFile.canWrite() && tempDirFile.canRead()))
                throw new Exception("Import-export temporary folder does not exist or lacks write permissions: "
                        + tempDir);

            Random rando = new Random();
            long id = Math.abs(rando.nextInt());

            headerFile = new File(tempDir, getNameForFile(dataset.getName()) + id + ".header");
            dataFile = new File(tempDir, getNameForFile(dataset.getName()) + id + ".data");

            splitFile(file, headerFile, dataFile);
            loadDataset(getComments(headerFile), dataset);

            String copyString = "COPY " + getFullTableName(table) + " (" + getColNames(fileFormat) + ") FROM '"
                    + putEscape(dataFile.getAbsolutePath()) + "' WITH CSV QUOTE AS '\"'";

            connection = datasource.getConnection();
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.execute(copyString);
            statement.close();
        } catch (Exception exc) {
            // NOTE: this closes the db server for other importers
            // try
            // {
            // if ((connection != null) && !connection.isClosed()) connection.close();
            // }
            // catch (Exception ex)
            // {
            // throw ex;
            // }
            // throw exc;
            throw new Exception(exc.getMessage());
        } finally {
            if ((headerFile != null) && headerFile.exists())
                headerFile.delete();
            if ((dataFile != null) && dataFile.exists())
                dataFile.delete();
        }
    }

    private void splitFile(File file, File headerFile, File dataFile) throws IOException, InterruptedException {
        if (windowsOS) {
            splitOnWindows(file, headerFile, dataFile);
            return;
        }

        String headerCmd = "grep \"^#\" " + file.getAbsolutePath() + " | grep -v \"^#EXPORT_\" > "
                + headerFile.getAbsolutePath();
        String dataCmd = "grep -v \"^#\" " + file.getAbsolutePath() + " | grep -v \"^[[:space:]]*$\" > "
                + dataFile.getAbsolutePath();
        String[] cmd = new String[] { "sh", "-c", headerCmd + ";" + dataCmd };

        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();
    }

    private void splitOnWindows(File file, File headerFile, File dataFile) throws IOException {
        BufferedReader fileReader = null;
        PrintWriter headWriter = null;
        PrintWriter dataWriter = null;
        String line = null;
        boolean firstHeadLine = true;
        boolean firstDataLine = true;
        headerFile.createNewFile();
        dataFile.createNewFile();

        try {
            fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(file)));
            headWriter = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(headerFile)));
            dataWriter = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(dataFile)));
        } catch (UnsupportedEncodingException e) {
            throw new FileNotFoundException("Encoding char set not supported.");
        }

        while ((line = fileReader.readLine()) != null) {
            line = line.trim();
            if (line.startsWith("#") && !line.startsWith("#EXPORT_")) {
                if (!firstHeadLine)
                    headWriter.println();
                headWriter.write(line);
                firstHeadLine = false;
            } else if (!line.startsWith("#") && !line.isEmpty()) {
                if (!firstDataLine)
                    dataWriter.println();
                dataWriter.write(line);
                firstDataLine = false;
            }
        }

        fileReader.close();
        headWriter.close();
        dataWriter.close();
    }

    private List<String> getComments(File file) throws Exception {
        BufferedReader fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(file)));
        List<String> commentsList = new ArrayList<String>();
        String line = null;

        while ((line = fileReader.readLine()) != null)
            commentsList.add(line);

        fileReader.close();

        return commentsList;
    }

    private String getFullTableName(String table) {
        return this.datasource.getName() + "." + table;
    }

    private String getColNames(FileFormatWithOptionalCols fileFormat) throws ImporterException {
        Column[] cols = fileFormat.cols();
        String colsString = "";

        try {
            for (int i = 0; i < record.size(); i++)
                colsString += cols[i].name() + ",";
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new ImporterException("Data doesn't match file format on number of columns " + "(expected:"
                    + cols.length + " but was:" + record.size() + ").");
        }

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
        
        if (!comments.hasContent("EMF_START_DATE") && !comments.hasContent("EMF_END_DATE"))
            setStartStopDateTimes(dataset, Integer.parseInt(year));
    }

    private void setStartStopDateTimes(Dataset dataset, int year) {
        Date start = new GregorianCalendar(year, Calendar.JANUARY, 1).getTime();
        dataset.setStartDateTime(start);

        Calendar endCal = new GregorianCalendar(year, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        dataset.setStopDateTime(endCal.getTime());
    }

    private String putEscape(String path) {
        if (windowsOS)
            return path.replaceAll("\\\\", "\\\\\\\\");

        return path;
    }

}
