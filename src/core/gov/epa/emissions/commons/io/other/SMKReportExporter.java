package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.InternalSource;

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
    
    private boolean formatted;
    
    private String tableframe;

    public SMKReportExporter(Dataset dataset, Datasource datasource) {
        this.dataset = dataset;
        this.datasource = datasource;
        setDelimiter(";");
        setFormatted(false);
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
            writeHeaders(writer, dataset);
            writeData(writer, dataset, datasource);
        } catch (SQLException e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String desc = dataset.getDescription();
        if(desc.lastIndexOf('#')+2 == desc.length()) {
            StringTokenizer st = new StringTokenizer(desc, System.getProperty("line.separator"));
            while(st.hasMoreTokens()){
                tableframe = st.nextToken();
            }
            writer.print(desc.substring(0, desc.indexOf(tableframe)));
        } else 
            writer.print(desc);
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = dataset.getInternalSources()[0];

        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ResultSet data = q.executeQuery("SELECT * FROM " + qualifiedTable);
        String[] cols = getCols(data);
        while (data.next())
            writeRecord(cols, data, writer);
        if(tableframe != null)
            writer.println(System.getProperty("line.separator") + tableframe);
    }

    protected void writeRecord(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 2; i < cols.length; i++) {
            if(formatted) {
                if(cols[i-1].equalsIgnoreCase("sccdesc")) //cols index is one less than table column index
                    writer.print("\"" + data.getObject(i).toString() + "\"");
                else 
                    writer.print(data.getObject(i).toString());
            }
            else {
                if(cols[i-1].equalsIgnoreCase("sccdesc"))
                    writer.print("\"" + data.getObject(i).toString().trim() + "\"");
                else
                    writer.print(data.getObject(i).toString().trim());
            }
            
            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
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
    
    public void setFormatted(boolean formatted) {
        this.formatted = formatted;
    }
}
