package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.CustomCharSetOutputStreamWriter;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.VersionedQuery;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.util.CustomDateFormat;

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

public class SMKReportExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private Datasource emfDatasource;

    private String delimiter;

    private String tableframe;

    private DataFormatFactory dataFormatFactory;

    private int batchSize;

    private long exportedLinesCount = 0;

    private String inlineCommentChar;

    protected int startColNumber;

    protected List<Integer> colTypes = new ArrayList<Integer>();

    protected List<String> colNames = new ArrayList<String>();

    public SMKReportExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        setup(dataset, dbServer, types, new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public SMKReportExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory factory,
            Integer optimizedBatchSize) {
        setup(dataset, dbServer, types, factory, optimizedBatchSize);
    }

    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory dataFormatFactory,
            Integer optimizedBatchSize) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.emfDatasource = dbServer.getEmfDatasource();
        this.dataFormatFactory = dataFormatFactory;
        this.batchSize = optimizedBatchSize.intValue();
        this.inlineCommentChar = dataset.getInlineCommentChar();
        setDelimiter(";");
    }

    public void export(File file) throws ExporterException {
        PrintWriter writer = null;
        try {
            // writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
            writer = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(file)));
            write(file, writer);
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        } catch (Exception e2) {
            e2.printStackTrace();
            throw new ExporterException(e2.getMessage());
        }
    }

    protected void write(File file, PrintWriter writer) throws Exception {
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
        } catch (Exception e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) throws SQLException {
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

        printExportInfo(writer);
    }

    private void printExportInfo(PrintWriter writer) throws SQLException {
        Version version = dataFormatFactory.getVersion();
        writer.println("#EXPORT_DATE=" + new Date().toString());
        writer.println("#EXPORT_VERSION_NAME=" + (version == null ? "None" : version.getName()));
        writer.println("#EXPORT_VERSION_NUMBER=" + (version == null ? "None" : version.getVersion()));

        writeRevisionHistories(writer, version);
    }

    private void writeRevisionHistories(PrintWriter writer, Version version) throws SQLException {
        VersionedQuery versionQuery = new VersionedQuery(version);
        DataQuery query = datasource.query();
        String revisionsTable = emfDatasource.getName() + ".revisions";
        String[] revisionsTableCols = { "date_time", "what", "why" };
        String usersTable = emfDatasource.getName() + ".users";
        String[] userCols = { "name" };
        String revisionsHistoryQuery = versionQuery.revisionHistoryQuery(revisionsTableCols, revisionsTable, userCols,
                usersTable);

        if (revisionsHistoryQuery == null || revisionsHistoryQuery.isEmpty())
            return;

        ResultSet data = null;

        try {
            data = query.executeQuery(revisionsHistoryQuery);

            while (data.next())
                writer.println("#REV_HISTORY " + CustomDateFormat.format_MM_DD_YYYY(data.getDate(1)) + " "
                        + data.getString(4) + ".    What: " + data.getString(2) + "    Why: " + data.getString(3));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (data != null)
                data.close();
        }
    }

    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource) throws Exception {
        writeData(writer, dataset, datasource, true);
    }

    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws Exception {
        writeData(writer, dataset, datasource, false);
    }

    private void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws Exception {
        boolean csvHeaderLine = dataset.getCSVHeaderLineSetting();
        String query = getQueryString(dataset, datasource);
        OptimizedQuery runner = datasource.optimizedQuery(query, batchSize);
        boolean firstbatch = true;
        String[] cols = null;

        int pad = 0;
        if (!comments) {
            pad = 1; // Add a comment column to header line
        }

        if (!csvHeaderLine)
            pad = 2; // Turn off head line

        while (runner.execute()) {
            ResultSet rs = runner.getResultSet();

            if (firstbatch) {
                getCols(rs);
                cols = this.colNames.toArray(new String[0]);
                this.startColNumber = startCol(cols);
                firstbatch = false;
            }

            if (pad < 2)
                writeCols(writer, cols, pad);

            writeBatchOfData(writer, rs, cols, comments);
            pad = 2;
            rs.close();
        }
        runner.close();
        if (tableframe != null)
            writer.println(System.getProperty("line.separator") + tableframe);
    }

    private void writeBatchOfData(PrintWriter writer, ResultSet data, String[] cols, boolean comments)
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
            String value = data.getString(i);

            if (value != null)
                writer.write(formatValue(cols, colTypes.get(i).intValue(), i, value));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
    }

    private String getQueryString(Dataset dataset, Datasource datasource) throws ExporterException {
        InternalSource source = dataset.getInternalSources()[0];
        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ExportStatement export = dataFormatFactory.exportStatement();

        return export.generate(qualifiedTable);
    }

    protected String formatValue(String[] cols, int colType, int index, String value) {
        if (cols[index - 1].toUpperCase().contains("DESCRIPTION"))
            return "\"" + value + "\"";

        if (cols[index - 1].equalsIgnoreCase("STATE"))
            return "\"" + value + "\"";

        if (cols[index - 1].equalsIgnoreCase("COUNTY"))
            return "\"" + value + "\"";

        if (containsDelimiter(value))
            return "\"" + value + "\"";

        return value;
    }

    protected String getComment(String value) {
        value = value.trim();
        if (value.equals(""))
            return value;

        if (!value.startsWith(inlineCommentChar))
            value = inlineCommentChar + value;

        return " " + value;
    }

    private void getCols(ResultSet data) throws SQLException {
        ResultSetMetaData md = data.getMetaData();
        colNames.clear();
        colTypes.clear();

        for (int i = 1; i <= md.getColumnCount(); i++) {
            colNames.add(md.getColumnName(i));
            colTypes.add(md.getColumnType(i));
        }
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
        if (isTableVersioned(cols))
            return 5;

        return 2;
    }

    protected boolean isTableVersioned(String[] cols) {
        return cols[2].equalsIgnoreCase("version") && cols[3].equalsIgnoreCase("delete_versions");
    }

    public long getExportedLinesCount() {
        return this.exportedLinesCount;
    }

}
