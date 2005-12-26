package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
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
import java.sql.SQLException;
import java.util.StringTokenizer;

public class TemporalProfileExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private TemporalFileFormatFactory factory;

    public TemporalProfileExporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.dataset = dataset;
        this.datasource = datasource;
        factory = new TemporalFileFormatFactory(sqlDataTypes);
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
            writeHeaders(writer, dataset);
            writeData(writer, dataset, datasource);
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

    private void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource[] sources = dataset.getInternalSources();

        for (int i = 0; i < sources.length; i++) {
            String table = sources[i].getTable();
            String qualifiedTable = datasource.getName() + "." + table;
            ResultSet data = q.executeQuery("SELECT * FROM " + qualifiedTable);
            Column[] cols = factory.get(table.replace('_', ' ')).cols();
            writer.println("/" + table + "/");

            while (data.next())
                writeRecord(cols, data, writer);

            writer.println("/END/");
        }
    }

    private void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            writer.print(cols[i].format(data));
            if (i + 1 < cols.length)
                writer.print(" ");// delimiter
        }
        writer.println();
    }
}
