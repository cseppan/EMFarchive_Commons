package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.CustomCharSetOutputStreamWriter;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ORLExporter extends GenericExporter {

    Log log = LogFactory.getLog(ORLExporter.class);

    private Dataset dataset;

    private Datasource datasource;

    private boolean windowsOS = false;

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, DataFormatFactory dataFormatFactory,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, fileFormat, dataFormatFactory, optimizedBatchSize);
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();

        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            windowsOS = true;
        setDelimiter(",");
    }

    public ORLExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, Integer optimizeBatchSize) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory(), optimizeBatchSize);
    }

    public void export(File file) throws ExporterException {
        // TBD: make this use the new temp dir
        String dataFileName = file.getAbsolutePath() + ".dat";
        String headerFileName = file.getAbsolutePath() + ".hed";
        File dataFile = new File(dataFileName);
        File headerFile = new File(headerFileName);        
        Connection connection = null;
       

        try {
            createNewFile(dataFile);
            writeHeader(headerFile);

            String originalQuery = getQueryString(dataset, datasource);
            String query = getColsSpecdQueryString(dataset, originalQuery);
            String writeQuery = getWriteQueryString(dataFileName, query);
            log.warn(writeQuery);

            connection = datasource.getConnection();

            executeQuery(connection, writeQuery);
            concatFiles(file, headerFileName, dataFileName);
            setExportedLines(originalQuery, connection);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if ((connection != null) && !connection.isClosed()) connection.close();                
            }
            catch (Exception ex) {
                ex.printStackTrace();
                throw new ExporterException(ex.getMessage());               
            }
            throw new ExporterException(e.getMessage());
        }
        finally
        {
            if (dataFile.exists()) dataFile.delete();
            if (headerFile.exists()) headerFile.delete();
        }
    }

    private void createNewFile(File file) throws Exception {
        try {
            if (windowsOS)
            {
                // AME: Updates for EPA's system
                file.createNewFile();
                Runtime.getRuntime().exec("CACLS " + file.getAbsolutePath() + " /E /G \"Users\":W");
                file.setWritable(true, false);
                Thread.sleep(1000); // for the system to refresh the file access permissions
            }
            //  for now, do nothing from Linux
        } catch (IOException e) {
            throw new ExporterException("Could not create export file: " + file.getAbsolutePath());
        }
    }

    private String putEscape(String path) {
        if (windowsOS)
            return path.replaceAll("\\\\", "\\\\\\\\");
        
        return path;
    }

    protected void writeHeader(File file) throws Exception {
        PrintWriter writer = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(file)));

        try {
            boolean headercomments = dataset.getHeaderCommentsSetting();

            if (headercomments)
                writeHeaders(writer, dataset);
        } finally {
            writer.close();
        }
    }

    private void executeQuery(Connection connection, String writeQuery) throws SQLException {
        Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.execute(writeQuery);
        statement.close();
    }

    private void concatFiles(File file, String headerFile, String dataFile) throws Exception {
        String[] cmd = null;

        if (windowsOS) {
            cmd = getCommands("copy " + headerFile + " + " + dataFile + " " + file.getAbsolutePath() + " /Y");
        } else {
            String cmdString = "cat " + headerFile + " " + dataFile + " > " + file.getAbsolutePath();
            cmd = new String[] { "sh", "-c", cmdString };
        }

        Process p = Runtime.getRuntime().exec(cmd);
        p.waitFor();

        new File(headerFile).delete();
        new File(dataFile).delete();
    }

    private String[] getCommands(String command) {
        String[] cmd = new String[3];
        String os = System.getProperty("os.name");

        if (os.equalsIgnoreCase("Windows 98") || os.equalsIgnoreCase("Windows 95")) {
            cmd[0] = "command.com";
        } else {
            cmd[0] = "cmd.exe";
        }

        cmd[1] = "/C";
        cmd[2] = command;

        return cmd;
    }

    private String getColsSpecdQueryString(Dataset dataset, String originalQuery) {
        String selectColsString = "SELECT ";
        Column[] cols = fileFormat.cols();
        int numCols = cols.length;

        for (int i = 0; i < numCols; i++)
            selectColsString += cols[i].name() + ",";

        selectColsString = selectColsString.substring(0, selectColsString.length() - 1);

        return selectColsString + " " + getSubString(originalQuery, "FROM", false);
    }

    private String getWriteQueryString(String dataFile, String query) {
        String withClause = " WITH NULL '' CSV FORCE QUOTE " + getNeedQuotesCols();

        return "COPY (" + query + ") to '" + putEscape(dataFile) + "'" + withClause;
    }

    private String getNeedQuotesCols() {
        String colNames = "";
        Column[] cols = fileFormat.cols();
        int numCols = cols.length;

        for (int i = 0; i < numCols; i++) {
            String colType = cols[i].sqlType().toUpperCase();

            if ((colType.startsWith("VARCHAR") || colType.startsWith("TEXT")) && cols[i].width() > 10)
                colNames += cols[i].name() + ",";
        }

        return (colNames.length() > 0) ? colNames.substring(0, colNames.length() - 1) : colNames;
    }

    public void setExportedLines(String originalQuery, Connection connection) throws SQLException {
        Date start = new Date();

        Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String fromClause = getSubString(originalQuery, "FROM", false);
        String queryCount = "SELECT COUNT(\"dataset_id\") " + getSubString(fromClause, "ORDER BY", true);
        ResultSet rs = statement.executeQuery(queryCount);
        rs.next();
        this.exportedLinesCount = rs.getLong(1);
        statement.close();

        Date ended = new Date();
        log.warn("Time used to count exported data lines(second): " + (ended.getTime() - start.getTime()) / 1000.00);
    }

    private String getSubString(String origionalString, String mark, boolean beforeMark) {
        int markIndex = origionalString.indexOf(mark);

        if (markIndex < 0)
            return origionalString;

        if (beforeMark)
            return origionalString.substring(0, markIndex);

        return origionalString.substring(markIndex);
    }

}
