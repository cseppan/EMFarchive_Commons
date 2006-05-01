package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
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

public class SMKReportExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private String delimiter;

    private String tableframe;

    private DataFormatFactory dataFormatFactory;

    private int batchSize;

    public SMKReportExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        setup(dataset, dbServer, types, new NonVersionedDataFormatFactory(),optimizedBatchSize);
    }

    public SMKReportExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory factory, Integer optimizedBatchSize) {
        setup(dataset, dbServer, types, factory,optimizedBatchSize);
    }

    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = dataFormatFactory;
        this.batchSize = optimizedBatchSize.intValue();
        setDelimiter(";");
    }

    public void export(File file) throws ExporterException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        }

        write(file, writer);
    }

    protected void write(File file, PrintWriter writer) throws ExporterException {
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

    protected void writeWithInlineComments(File file, PrintWriter writer) throws ExporterException {
        try {
            writeDataWithComments(writer, dataset, datasource);
        } catch (SQLException e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String desc = dataset.getDescription();
        if (desc != null) {
            if (desc.lastIndexOf('#') + 2 == desc.length()) {
                StringTokenizer st = new StringTokenizer(desc, System.getProperty("line.separator"));
                while (st.hasMoreTokens()) {
                    tableframe = st.nextToken();
                }
                writer.print(desc.substring(0, desc.indexOf(tableframe)));
            } else
                writer.print(desc);
        }
    }

    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, true);
    }

    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, false);
    }

    private void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws SQLException {
        String query = getQueryString(dataset, datasource);
        OptimizedQuery runner = datasource.optimizedQuery(query,batchSize);

        int pad = 0;
        if (!comments) {
            pad = 1;
        }

        while (runner.execute()) {
            ResultSet rs = runner.getResultSet();
            
            if (pad < 2)
                writeCols(writer, getCols(rs), pad);
            
            writeBatchOfData(writer, rs, comments);
            pad = 2;
        }
        runner.close();
        if (tableframe != null)
            writer.println(System.getProperty("line.separator") + tableframe);
    }

    private void writeBatchOfData(PrintWriter writer, ResultSet data, boolean comments) throws SQLException {
        String[] cols = getCols(data);

        if (comments) {
            writeComments(writer, data, cols);
            return;
        }
        writeDataWithoutComments(writer, data, cols);
    }

    private void writeDataWithoutComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(data, writer, cols, 0);
        data.close();
    }

    private void writeComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(data, writer, cols, 1);
        data.close();
    }

    private String getQueryString(Dataset dataset, Datasource datasource) {
        InternalSource source = dataset.getInternalSources()[0];
        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ExportStatement export = dataFormatFactory.exportStatement();

        return export.generate(qualifiedTable);
    }

    private void writeRecord(ResultSet data, PrintWriter writer, String[] cols, int commentspad) throws SQLException {
        int i = startCol(cols);
        for (; i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            if (value != null)
                writer.write(getValue(cols, i, value));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
    }

    protected String getValue(String[] cols, int index, String value) {
        if (!isComment(index, cols))
            return formatValue(cols, index, value);

        return getComment(value);
    }

    protected String formatValue(String[] cols, int index, String value) {
        if (cols[index - 1].equalsIgnoreCase("sccdesc") || containsDelimiter(value))
            return "\"" + value + "\"";

        return value;
    }

    protected String getComment(String value) {
        value = value.trim();
        if (value.equals(""))
            return value;

        if (!value.startsWith(dataset.getInlineCommentChar()))
            value = dataset.getInlineCommentChar() + value;

        return " " + value;
    }

    protected boolean isComment(int index, String[] cols) {
        return (index == cols.length);
    }

    private String[] getCols(ResultSet data) throws SQLException {
        List cols = new ArrayList();
        ResultSetMetaData md = data.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
            cols.add(md.getColumnName(i));

        return (String[]) cols.toArray(new String[0]);
    }

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

    private boolean containsDelimiter(String s) {
        return s.indexOf(delimiter) >= 0;
    }

    private void writeCols(PrintWriter writer, String[] cols, int pad) {
        int i = startCol(cols) - 1;
        for (; i < cols.length - pad; i++) {
            writer.print(cols[i]);
            if (i + 1 + pad < cols.length)
                writer.print(delimiter);// delimiter
        }

        writer.println();
    }

    protected int startCol(String[] cols) {
        int i = 2;
        if (isTableVersioned(cols))
            i = 5;

        return i;
    }

    protected boolean isTableVersioned(String[] cols) {
        return cols[2].equalsIgnoreCase("version") && cols[3].equalsIgnoreCase("delete_versions");
    }

}
