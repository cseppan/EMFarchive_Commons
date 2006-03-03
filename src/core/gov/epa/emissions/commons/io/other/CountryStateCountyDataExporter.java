package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.InternalSource;
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

    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes) {
        setup(dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }
    
    public CountryStateCountyDataExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory) {
        setup(dataset, dbServer, types, factory);
    }
    
    private void setup(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory dataFormatFactory) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.dataFormatFactory = dataFormatFactory;
        setDelimiter(" ");
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
            
            if(headercomments && inlinecomments) {
                writeHeaders(writer, dataset);
                writeDataWithComments(writer, dataset, datasource);
            }
            
            if(headercomments && !inlinecomments) {
                writeHeaders(writer, dataset);
                writeDataWithoutComments(writer, dataset, datasource);
            }
            
            if(!headercomments && inlinecomments) {
                writeDataWithComments(writer, dataset, datasource);
            }
            
            if(!headercomments && !inlinecomments) {
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

        if(header != null){
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()){
                writer.println("#" + st.nextToken());
            }
        }
    }

    protected void writeDataWithComments(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for(int i = 0; i < sources.length; i++){
            ResultSet data = getResultSet(sources[i], q);
            String[] cols = getCols(data);
            writer.println("/" + sources[i].getTable() + "/");

            while (data.next())
                writeRecordWithComments(cols, data, writer);
        }
    }
    
    protected void writeDataWithoutComments(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for(int i = 0; i < sources.length; i++){
            ResultSet data = getResultSet(sources[i], q);
            String[] cols = getCols(data);
            writer.println("/" + sources[i].getTable() + "/");

            while (data.next())
                writeRecordWithoutComments(cols, data, writer);
        }
    }
    
    protected ResultSet getResultSet(InternalSource source, DataQuery q) throws SQLException {
        String table = source.getTable();
        String qualifiedTable = datasource.getName() + "." + table;
        ExportStatement export = dataFormatFactory.exportStatement();
        ResultSet data = q.executeQuery(export.generate(qualifiedTable));
        
        return data;
    }
    
    protected String[] getCols(ResultSet data) throws SQLException {
        List cols = new ArrayList();
        ResultSetMetaData md = data.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
            cols.add(md.getColumnName(i));
  
        return (String[]) cols.toArray(new String[0]);
     } 

    protected void writeRecordWithComments(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        writeRecord(cols, data, writer, 1);
    }
    
    protected void writeRecordWithoutComments(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        writeRecord(cols, data, writer, 0);
    }

    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer, int commentspad) throws SQLException {
        int i = startCol(cols) + 1;
        for (; i < cols.length + commentspad; i++) {
            if(data.getObject(i) != null) {
                String colValue = data.getObject(i).toString().trim();
                if(i == cols.length && !colValue.equals("")) {
                    if(colValue.charAt(0) == dataset.getInlineCommentChar())
                        writer.print(" " + colValue);
                    else
                        writer.print(" " + dataset.getInlineCommentChar() + colValue);
                } else {
                    writer.print(colValue);
                }

                if (i + 1 < cols.length)
                    writer.print(delimiter);// delimiter
            }
        }
        writer.println();
    }
    
    private int startCol(String[] cols) {
        int i = 1;
        if(cols[2].equalsIgnoreCase("version") && cols[3].equalsIgnoreCase("delete_versions"))
            i = 4;
        
        return i;
    }

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

}
