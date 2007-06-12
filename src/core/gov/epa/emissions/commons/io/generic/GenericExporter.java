package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.CustomCharSetOutputStreamWriter;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

public class GenericExporter implements Exporter {

    private Dataset dataset;

    private Datasource datasource;

    protected String delimiter;

    private DataFormatFactory dataFormatFactory;

    protected FileFormat fileFormat;

    protected String inlineCommentChar;

    private int batchSize;

    protected int startColNumber = 2; // shifted by "obj_id","record_id" when write data

    protected long exportedLinesCount = 0;

    public GenericExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat, Integer optimizedBatchSize) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public GenericExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = dataFormatFactory;
        this.fileFormat = fileFormat;
        this.batchSize = optimizedBatchSize.intValue();
        this.inlineCommentChar = dataset.getInlineCommentChar();

        setDelimiter(";");
    }

    public void export(File file) throws ExporterException {
        try {
            PrintWriter writer = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(file)));
            write(file, writer);
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        }
    }

    final protected void write(File file, PrintWriter writer) throws ExporterException {
        try {
            boolean headercomments = dataset.getHeaderCommentsSetting();
            boolean inlinecomments = dataset.getInlineCommentSetting();

            if (headercomments && inlinecomments) {
                writeHeaders(writer, dataset);
                writeDataWithComments(writer, dataset, datasource);
            }

            if (headercomments && !inlinecomments) {
                writeHeaders(writer, dataset);
                writeDataWithoutComments(writer, dataset, datasource);
            }

            if (!headercomments && inlinecomments) {
                writeDataWithComments(writer, dataset, datasource);
            }

            if (!headercomments && !inlinecomments) {
                writeDataWithoutComments(writer, dataset, datasource);
            }
        } catch (SQLException e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String header = dataset.getDescription();
        String cr = System.getProperty("line.separator");

        if (header != null) {
            StringTokenizer st = new StringTokenizer(header, "#");
            String lasttoken = "";
            while (st.hasMoreTokens()) {
                lasttoken = st.nextToken();
                writer.print("#" + lasttoken);
            }

            if (lasttoken.indexOf(cr) < 0)
                writer.print(cr);
        }
        
        printExportInfo(writer);
    }

    private void printExportInfo(PrintWriter writer) {
        Version version = dataFormatFactory.getVersion();
        writer.println("#EXPORT_DATE=" + new Date().toString());
        writer.println("#EXPORT_VERSION_NAME=" + (version == null ? "None" : version.getName()));
        writer.println("#EXPORT_VERSION_NUMBER=" + (version == null ? "None" : version.getVersion()));
    }

    final protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, true);
    }

    final protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, false);
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws SQLException {
        String query = getQueryString(dataset, datasource);
        OptimizedQuery runner = datasource.optimizedQuery(query, batchSize);
        boolean firstbatch = true;
        String[] cols = null;

        try {
            while (runner.execute()) {
                ResultSet resultSet = runner.getResultSet();

                if (firstbatch) {
                    cols = getCols(resultSet);
                    this.startColNumber = startCol(cols);
                    firstbatch = false;
                }

                writeBatchOfData(writer, resultSet, cols, comments);
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SQLException("Error in executing export query. Check the sort order in the dataset type.\n"
                    + e.getMessage());
        }

        runner.close();
    }

    protected String getQueryString(Dataset dataset, Datasource datasource) {
        InternalSource source = dataset.getInternalSources()[0];
        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ExportStatement export = dataFormatFactory.exportStatement();

        return export.generate(qualifiedTable);
    }

    protected void writeBatchOfData(PrintWriter writer, ResultSet data, String[] cols, boolean comments)
            throws SQLException {
        if (comments)
            writeWithComments(writer, data, cols);
        else
            writeWithoutComments(writer, data, cols);
    }

    private void writeWithoutComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecordWithoutComment(cols, data, writer);
    }

    private void writeWithComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecordWithComment(cols, data, writer);
    }

    protected void writeRecordWithComment(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        writeDataCols(cols, data, writer);
        String value = data.getString(cols.length);
        writer.write(value == null ? "" : getComment(value));

        writer.println();
        ++exportedLinesCount;
    }

    protected void writeRecordWithoutComment(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        writeDataCols(cols, data, writer);
        writer.println();
        ++exportedLinesCount;
    }

    protected void writeDataCols(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = startColNumber; i < cols.length; i++) {
            writer.write(formatValue(i, data));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
    }

    protected String formatValue(int index, ResultSet data) throws SQLException {
        Column column = fileFormat.cols()[index - startColNumber];
        return (delimiter.equals("")) ? getFixedPositionValue(column, data) : getDelimitedValue(column, data);
    }

    final protected String getDelimitedValue(Column column, ResultSet data) throws SQLException {
        String colType = column.sqlType().toUpperCase();
        String val = data.getString(column.name());

        if (val == null || val.equals(""))
            return "";

        if ((colType.startsWith("VARCHAR") || colType.startsWith("TEXT")) && column.width() > 10)
            return "\"" + val + "\"";

        return val;
    }

    final protected String getFixedPositionValue(Column column, ResultSet data) throws SQLException {
        return column.format(data);
    }

    final protected String getComment(String value) {
        value = value.trim();
        if (value.equals(""))
            return value;

        if (!value.startsWith(inlineCommentChar))
            value = inlineCommentChar + value;

        return value;
    }

    private String[] getCols(ResultSet data) throws SQLException {
        List cols = new ArrayList();
        ResultSetMetaData md = data.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
            cols.add(md.getColumnName(i));

        return (String[]) cols.toArray(new String[0]);
    }

    protected int startCol(String[] cols) {
        if (isTableVersioned(cols))
            return 5; // shifted by "Obj_Id", "Record_Id",
        // "Dataset_Id", "Version", "Delete_Versions"

        return 2; // shifted by "Obj_Id", "Record_Id"
    }

    final protected boolean isTableVersioned(String[] cols) {
        return cols[2].equalsIgnoreCase("version") && cols[3].equalsIgnoreCase("delete_versions");
    }

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

    final protected String getDelimiter() {
        return delimiter;
    }

    public long getExportedLinesCount() {
        return this.exportedLinesCount;
    }

}
