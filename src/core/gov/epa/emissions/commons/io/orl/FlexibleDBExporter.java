package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.data.KeyVal;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.CustomCharSetOutputStreamWriter;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.XFileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlexibleDBExporter extends GenericExporter {

    Log log = LogFactory.getLog(ORLExporter.class);

    private Dataset dataset;
    
    private XFileFormat fileFormat;

    private Datasource datasource;

    private boolean windowsOS = false;
    
    private boolean withColNames = true;   //true: first data line is column names
    
    public FlexibleDBExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, DataFormatFactory dataFormatFactory,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, dataset.getDatasetType().getFileFormat(), dataFormatFactory, optimizedBatchSize);
        this.dataset = dataset;
        this.fileFormat = dataset.getDatasetType().getFileFormat();
        this.datasource = dbServer.getEmissionsDatasource();
        this.withColNames = getColumnLabel();

        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            windowsOS = true;
        setDelimiter(",");
    }

    public FlexibleDBExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizeBatchSize) {
        this(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory(), optimizeBatchSize);
    }

    public void export(File file) throws ExporterException {
        // TBD: make this use the new temp dir
        String tempDir = System.getProperty("IMPORT_EXPORT_TEMP_DIR");

        if (tempDir == null || tempDir.isEmpty())
            tempDir = System.getProperty("java.io.tmpdir");

        File tempDirFile = new File(tempDir);

        if (!(tempDirFile.exists() && tempDirFile.isDirectory() && tempDirFile.canWrite() && tempDirFile.canRead()))
            throw new ExporterException("Import-export temporary folder does not exist or lacks write permissions: "
                    + tempDir + "");
        
        if (fileFormat==null || fileFormat.cols()== null || fileFormat.cols().length == 0)
            throw new ExporterException("Flexible file format is not defined for dataset: "+dataset.getName());

        Random rando = new Random();
        long id = Math.abs(rando.nextInt());

        String separator = System.getProperty("file.separator");
        String dataFileName = tempDir + separator + file.getName() + id + ".dat";
        String colsFileName = tempDir + separator + file.getName() + id + ".col";
        System.out.println("File name: "+ colsFileName);
        String headerFileName = tempDir + separator + file.getName() + id + ".hed";
        File dataFile = new File(dataFileName);
        File headerFile = new File(headerFileName);

        // use one statement and connection object for all operations, this we can easily clean them up....
        Connection connection = null;
        Statement statement = null;

        try {
            createNewFile(dataFile);
            writeHeader(headerFile);

            String originalQuery = getQueryString(dataset, datasource);
            String query = getColsSpecdQueryString(dataset, originalQuery);
            String writeQuery = getWriteQueryString(dataFileName, query);

            // log.warn(writeQuery);

            connection = datasource.getConnection();

            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            executeQuery(statement, writeQuery);
            concatFiles(file, headerFileName, dataFileName);
            setExportedLines(originalQuery, statement);
        } catch (Exception e) {
            e.printStackTrace();
            // NOTE: this closes the db server for other exporters
            // try {
            // if ((connection != null) && !connection.isClosed())
            // connection.close();
            // } catch (Exception ex) {
            // ex.printStackTrace();
            // throw new ExporterException(ex.getMessage());
            // }
            throw new ExporterException(e.getMessage());
        } finally {
            try {
                if (statement != null)
                    statement.close();
                // if (connection != null)
                // connection.close();
            } catch (SQLException e) {
                //
            }
            if (dataFile.exists())
                dataFile.delete();
            if (headerFile.exists())
                headerFile.delete();
        }
    }

    private Map<String, String> getTableCols(Dataset dataset) {
        ResultSet rs = null;
        Map<String, String> cols = new HashMap<String, String>();
        InternalSource source = dataset.getInternalSources()[0];
        String qualifiedTable = datasource.getName() + "." + source.getTable();
        try {
            rs = datasource.query().executeQuery("select * from " + qualifiedTable + " where 1 = 0");
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++)
                cols.put(md.getColumnName(i).toLowerCase(), md.getColumnName(i).toLowerCase());
        } catch (SQLException e) {
            //
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    //
                }
        }

        return cols;
    }

    private void createNewFile(File file) throws Exception {
        try {
            if (windowsOS) {
                // AME: Updates for EPA's system
                file.createNewFile();
                Runtime.getRuntime().exec("CACLS " + file.getAbsolutePath() + " /E /G \"Users\":W");
                file.setWritable(true, false);
                Thread.sleep(1000); // for the system to refresh the file access permissions
            }
            // for now, do nothing from Linux
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

    private void executeQuery(Statement statement, String writeQuery) throws SQLException {
        // Statement statement = null;

        try {
            // statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.execute(writeQuery);
        } catch (Exception e) {
            log.error("Error executing query: " + writeQuery + ".", e);
            throw new SQLException(e.getMessage());
        } finally {
            // if (statement != null)
            // statement.close();
        }
    }

    private void concatFiles(File file, String headerFile, String dataFile) throws Exception {
        String[] cmd = null;

        if (windowsOS)
            cmd = getCommands("copy " + headerFile + " + " +dataFile + " " + file.getAbsolutePath() + " /Y");
        else {
            String cmdString = "cat " + headerFile + " " + dataFile + " > " + file.getAbsolutePath();
            cmd = new String[] { "sh", "-c", cmdString };
        }
        
        System.out.println(cmd);

        Process p = Runtime.getRuntime().exec(cmd);
        int errorLevel = p.waitFor();

        if (errorLevel > 0)
            throw new Exception("Concatinating header and ORL data to file " + file.getAbsolutePath() + " failed.");
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
        Map<String, String> tableColsMap = getTableCols(dataset);
        int numCols = cols.length;

        for (int i = 0; i < numCols; i++) {
            String colName = cols[i].name().toLowerCase();
            // make sure you only include columns that exist in the table, new columns could have been
            // added to the ORL file format...
            selectColsString += (tableColsMap.containsKey(colName) ? colName : "null as " + colName) + ",";
        }

        selectColsString = selectColsString.substring(0, selectColsString.length() - 1);

        return selectColsString + " " + getSubString(originalQuery, "FROM", false);
    }

    private String getWriteQueryString(String dataFile, String query) {
        String withClause = " WITH NULL '' CSV FORCE QUOTE " + getNeedQuotesCols();
        if (withColNames)
            withClause = " WITH NULL '' CSV HEADER FORCE QUOTE " + getNeedQuotesCols();
        System.out.println("COPY (" + query + ") to '" + putEscape(dataFile) + "'" + withClause);
        return "COPY (" + query + ") to '" + putEscape(dataFile) + "'" + withClause;
    }

    private String getNeedQuotesCols() {
        String colNames = "";
        Column[] cols = fileFormat.cols();
        int numCols = cols.length;

        for (int i = 0; i < numCols; i++) {
            String colType = cols[i].sqlType().toUpperCase();

            if (colType.startsWith("VARCHAR") || colType.startsWith("TEXT"))
                colNames += cols[i].name() + delimiter;
        }

        return (colNames.length() > 0) ? colNames.substring(0, colNames.length() - 1) : colNames;
    }

    public void setExportedLines(String originalQuery, Statement statement) throws SQLException {
        Date start = new Date();

        // Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        // ResultSet.CONCUR_READ_ONLY);
        String fromClause = getSubString(originalQuery, "FROM", false);
        String queryCount = "SELECT COUNT(\"dataset_id\") " + getSubString(fromClause, "ORDER BY", true);
        ResultSet rs = statement.executeQuery(queryCount);
        rs.next();
        this.exportedLinesCount = rs.getLong(1);
        statement.close();

        Date ended = new Date();
        double lapsed = (ended.getTime() - start.getTime()) / 1000.00;

        if (lapsed > 5.0)
            log.warn("Time used to count exported data lines(second): " + lapsed);
    }

    private String getSubString(String origionalString, String mark, boolean beforeMark) {
        int markIndex = origionalString.indexOf(mark);

        if (markIndex < 0)
            return origionalString;

        if (beforeMark)
            return origionalString.substring(0, markIndex);

        return origionalString.substring(markIndex);
    }
    
    private boolean  getColumnLabel(){
        KeyVal[] keys = keyValFound(Dataset.csv_header_line);
        if (keys !=null && keys.length >0){
            String value = keys[0].getValue().toLowerCase();
            if ( value !=null && (value.contains("n") || value.contains("f"))) 
                return false;              //first line of data file is data 
        }
        return true; 
    }
    
    private  KeyVal[] keyValFound(String keyword) {
        KeyVal[] keys = dataset.getDatasetType().getKeyVals();
        List<KeyVal> list = new ArrayList<KeyVal>();
        
        for (KeyVal key : keys)
            if (key.getName().equalsIgnoreCase(keyword)) 
                list.add(key);
        
        return list.toArray(new KeyVal[0]);
    }
    
//    private String getCols() {
//        String colNames = "";
//        Column[] cols = fileFormat.cols();
//        int numCols = cols.length;
//
//        for (int i = 0; i < numCols; i++) {
//            colNames += cols[i].name() + delimiter;
//        }
//
//        return (colNames.length() > 0) ? colNames.substring(0, colNames.length() - 1) : colNames;
//    }
    
}
