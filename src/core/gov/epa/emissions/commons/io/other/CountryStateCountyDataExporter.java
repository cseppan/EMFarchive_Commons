package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.InternalSource;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.OptimizedQuery;
import gov.epa.emissions.commons.db.SqlDataTypes;
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

public class CountryStateCountyDataExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private DataFormatFactory dataFormatFactory;

    private String delimiter;

    private FileFormat fileFormat;

    private SqlDataTypes types;

    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        setup(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        setup(dataset, dbServer, types, factory);
    }

    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types, DataFormatFactory dataFormatFactory) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = dataFormatFactory;
        this.types = types;
        setDelimiter("");
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

    private void write(File file, PrintWriter writer) throws ExporterException {
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
    }

    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, true);
    }

    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource)
            throws SQLException {
        writeData(writer, dataset, datasource, false);
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource, boolean comments)
            throws SQLException {
        InternalSource[] sources = dataset.getInternalSources();

        for (int i = 0; i < sources.length; i++) {
            writer.println("/" + sources[i].getTable() + "/");
            this.fileFormat = getFileFormat(sources[i].getTable());
            
            writeResultSet(writer, sources[i], datasource, comments);
        }
    }

    protected void writeResultSet(PrintWriter writer, InternalSource source, Datasource datasource, boolean comments) throws SQLException {
        String query = getQueryString(source, datasource);
        OptimizedQuery runner = datasource.optimizedQuery(query);

        while (runner.execute())
            writeBatchOfData(writer, runner.getResultSet(), comments);

        runner.close();
    }
    
    private void writeBatchOfData(PrintWriter writer, ResultSet data, boolean comments) throws SQLException {
        String[] cols = getCols(data);

        if (comments) {
            writeComments(writer, data, cols);
            return;
        }
        writeDataWithoutComments(writer, data, cols);
    }
    
    private void writeComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(cols, data, writer, 1);
        data.close();
    }
    
    private void writeDataWithoutComments(PrintWriter writer, ResultSet data, String[] cols) throws SQLException {
        while (data.next())
            writeRecord(cols, data, writer, 0);
        data.close();
    }

    protected FileFormat getFileFormat(String fileFormatName) {
        CountryStateCountyFileFormatFactory factory = new CountryStateCountyFileFormatFactory(types);

        return factory.get(fileFormatName);
    }

    private String getQueryString(InternalSource source, Datasource datasource) {
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

    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        for (int i = startCol(cols); i < cols.length + commentspad; i++) {
            String value = data.getString(i);
            writer.write(getValue(cols, i, value, data));

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
    }

    protected String getValue(String[] cols, int index, String value, ResultSet data) throws SQLException {
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

        if (!value.startsWith(dataset.getInlineCommentChar()))
            value = dataset.getInlineCommentChar() + value;

        return value;
    }

    protected boolean isComment(int index, String[] cols) {
        return (index == cols.length);
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

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

}
