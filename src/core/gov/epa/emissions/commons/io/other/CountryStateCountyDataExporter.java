package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
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

public class CountryStateCountyDataExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private DataFormatFactory dataFormatFactory;

    private String delimiter;

    protected FileFormat fileFormat;

    private SqlDataTypes types;

    private int batchSize;

    private long exportedLinesCount;

    private String inlineCommentChar;

    protected int startColNumber;

    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            Integer optimizedBatchSize) {
        setup(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        setup(dataset, dbServer, types, factory, optimizedBatchSize);
    }

    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory dataFormatFactory,
            Integer optimizedBatchSize) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = dataFormatFactory;
        this.types = types;
        this.batchSize = optimizedBatchSize.intValue();
        this.inlineCommentChar = dataset.getInlineCommentChar();
        setDelimiter("");
    }

    public void export(File file) throws ExporterException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new CustomCharSetOutputStreamWriter(new FileOutputStream(file)));
            write(file, writer);
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        } catch (Exception e2) {
            throw new ExporterException(e2.getMessage());
        }
    }

    private void write(File file, PrintWriter writer) throws Exception {
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

    private void writeHeaders(PrintWriter writer, Dataset dataset) {
        String header = dataset.getDescription();

        if (header != null) {
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()) {
                writer.println("#" + st.nextToken());
            }
        }
        
        printExportInfo(writer);
    }
    
    private void printExportInfo(PrintWriter writer) {
        Version version = dataFormatFactory.getVersion();
        writer.println("#EXPORT_DATE=" + new Date().toString());
        writer.println("#EXPORT_VERSION_NAME=" + (version == null ? "None" : version.getName()));
        writer.println("#EXPORT_VERSION_NUMBER=" + (version == null ? "None" : version.getVersion()));
    }


    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws Exception {
        writeData(writer, dataset, datasource, true);
    }

    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws Exception {
        writeData(writer, dataset, datasource, false);
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws Exception {
        InternalSource[] sources = dataset.getInternalSources();

        for (int i = 0; i < sources.length; i++) {
            String section = sources[i].getTable();
            writer.println("/" + section + "/");
            this.fileFormat = getFileFormat(sources[i].getTable());

            writeResultSet(writer, sources[i], datasource, comments, section);
        }
    }

    protected void writeResultSet(PrintWriter writer, InternalSource source, Datasource datasource, boolean comments, String section)
            throws Exception {
        String query = getQueryString(source, datasource);
        String orderby = "";
        
        if (section.toUpperCase().equals("COUNTRY"))
            orderby = " ORDER BY code";
        else if (section.toUpperCase().equals("STATE"))
            orderby = " ORDER BY countrycode, statecode";
        else if (section.toUpperCase().equals("COUNTY"))
            orderby = " ORDER BY countrycode, statecode, countycode";
            
        OptimizedQuery runner = datasource.optimizedQuery(query + orderby, batchSize);
        boolean firstbatch = true;
        String[] cols = null;

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

        runner.close();
    }

    private void writeBatchOfData(PrintWriter writer, ResultSet data, String[] cols, boolean comments)
            throws SQLException {
        if (comments)
            writeWithComments(writer, data, cols);
        else
            writeWithoutComments(writer, data, cols);
    }

    private void writeWithComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecordWithComment(cols, data, writer);
    }

    private void writeWithoutComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecordWithoutComment(cols, data, writer);
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
        return getFixedPositionValue(column, data);
    }

    protected String getDelimitedValue(Column column, ResultSet data) throws SQLException {
        String value = column.format(data).trim();

        if (column.sqlType().startsWith("FLOAT") || column.sqlType().startsWith("DOUBLE")) {
            value = "" + Float.parseFloat(value);
            if (value.endsWith(".0"))
                value = value.substring(0, value.lastIndexOf(".0"));
        }

        return value;
    }

    protected String getFixedPositionValue(Column column, ResultSet data) throws SQLException {
        String value = getDelimitedValue(column, data);
        String leadingSpace = "";
        int spaceCount = column.width() - value.length();

        for (int i = 0; i < spaceCount; i++)
            leadingSpace += " ";

        return leadingSpace + value;
    }

    protected String getComment(String value) {
        value = value.trim();
        if (value.equals(""))
            return value;

        if (!value.startsWith(inlineCommentChar))
            value = inlineCommentChar + value;

        return value;
    }

    protected FileFormat getFileFormat(String fileFormatName) {
        CountryStateCountyFileFormatFactory factory = new CountryStateCountyFileFormatFactory(types);

        return factory.get(fileFormatName);
    }

    private String getQueryString(InternalSource source, Datasource datasource) throws ExporterException {
        String table = source.getTable();
        String qualifiedTable = datasource.getName() + "." + table;
        ExportStatement export = dataFormatFactory.exportStatement();

        return export.generate(qualifiedTable);
    }

    protected String[] getCols(ResultSet data) throws SQLException {
        List cols = new ArrayList();
        ResultSetMetaData md = data.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
            cols.add(md.getColumnName(i));

        return (String[]) cols.toArray(new String[0]);
    }

    protected int startCol(String[] cols) {
        if (isTableVersioned(cols))
            return 5;

        return 2;
    }

    protected boolean isTableVersioned(String[] cols) {
        return cols[2].equalsIgnoreCase("version") && cols[3].equalsIgnoreCase("delete_versions");
    }

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

    public long getExportedLinesCount() {
        return this.exportedLinesCount;
    }

}
