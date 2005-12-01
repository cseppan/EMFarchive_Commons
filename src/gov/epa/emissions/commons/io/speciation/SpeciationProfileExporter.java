package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class SpeciationProfileExporter {
    private Dataset dataset;

    private Datasource datasource;

    private FileFormat fileFormat;

    public SpeciationProfileExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        this.dataset = dataset;
        this.datasource = datasource;
        this.fileFormat = fileFormat;
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
        String header = dataset.getDescription();

        if(header != null){
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()){
                writer.println("#" + st.nextToken());
            }
        }
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = dataset.getInternalSources()[0];

        ResultSet data = q.selectAll(source.getTable());
        Column[] cols = fileFormat.cols();
        while (data.next())
            writeRecord(cols, data, writer);
    }

    protected void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            writer.print(cols[i].format(data));
            if (i + 1 < cols.length)
                writer.print(" ");// delimiter
        }
        writer.println();
    }

}
