package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class GenericExporter implements Exporter {

    private Dataset dataset;

    private Datasource datasource;

    protected String delimiter;

    private DataFormatFactory dataFormatFactory;

    protected FileFormat fileFormat;

    private int batchSize;

    private long exportedLinesCount = 0;

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

        setDelimiter(";");
    }

    public void export(File file) throws ExporterException {
        try {
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
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

        try {
            while (runner.execute()) {
                ResultSet resultSet = runner.getResultSet();
                writeBatchOfData(writer, resultSet, comments);
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SQLException("Error in executing export query. Check the sort order in the dataset type.\n"
                    + e.getMessage());
        }

        runner.close();
    }

    protected void writeBatchOfData(PrintWriter writer, ResultSet data, boolean comments) throws SQLException {
        String[] cols = getCols(data);

        if (comments) {
            writeComments(writer, data, cols);
            return;
        }
        writeDataWithoutComments(writer, data, cols);
    }

    private void writeDataWithoutComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(cols, data, writer, 0);
    }

    private void writeComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(cols, data, writer, 1);
    }

    protected String getQueryString(Dataset dataset, Datasource datasource) {
        InternalSource source = dataset.getInternalSources()[0];
        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ExportStatement export = dataFormatFactory.exportStatement();

        return export.generate(qualifiedTable);
    }

    private String[] getCols(ResultSet data) throws SQLException {
        List cols = new ArrayList();
        ResultSetMetaData md = data.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
            cols.add(md.getColumnName(i));

        return (String[]) cols.toArray(new String[0]);
    }

    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        for (int i = startCol(cols); i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            writer.write(getValue(cols, i, value, data));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
        ++exportedLinesCount;
    }

    final protected String getValue(String[] cols, int index, String value, ResultSet data) throws SQLException {
        if (!isComment(index, cols))
            return formatValue(cols, index, data);

        if (value != null)
            return getComment(value);

        return "";
    }

    protected String formatValue(String[] cols, int index, ResultSet data) throws SQLException {
        int fileIndex = index;
        if (isTableVersioned(cols))
            fileIndex = index - 3;

        Column column = fileFormat.cols()[fileIndex - 2];
        return (delimiter.equals("")) ? getFixedPositionValue(column, data) : getDelimitedValue(column, data);
    }

    final protected String getDelimitedValue(Column column, ResultSet data) throws SQLException {
        // return column.format(data).trim();
        String colType = column.sqlType().toUpperCase();
        String val = data.getString(column.name());
        if (val == null)
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

        if (!value.startsWith(dataset.getInlineCommentChar()))
            value = dataset.getInlineCommentChar() + value;

        return value;
    }

    final protected boolean isComment(int index, String[] cols) {
        return (index == cols.length);
    }

    protected int startCol(String[] cols) {
        int i = 2;
        if (isTableVersioned(cols))
            i = 5;

        return i;
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
