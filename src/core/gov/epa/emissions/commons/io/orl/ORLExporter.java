package gov.epa.emissions.commons.io.orl;

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

public class ORLExporter {

    private Dataset dataset;

    private Datasource datasource;

    private FileFormat fileFormat;

    public ORLExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        this.dataset = dataset;
        this.datasource = datasource;
        this.fileFormat = fileFormat;
    }

    public void export(File file) throws ExporterException {
        export(0, file);
    }

    public void export(int version, File file) throws ExporterException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        }

        write(version, file, writer);
    }

    private void write(int version, File file, PrintWriter writer) throws ExporterException {
        try {
            writeHeaders(writer, dataset);
            writeData(version, writer, dataset, datasource);
        } catch (SQLException e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    private void writeHeaders(PrintWriter writer, Dataset dataset) {
        writer.println(dataset.getDescription());
    }

    private void writeData(int version, PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = dataset.getInternalSources()[0];

        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ResultSet data = q.executeQuery("SELECT * FROM " + qualifiedTable + " WHERE version=" + version);
        Column[] cols = fileFormat.cols();
        while (data.next())
            writeRecord(cols, data, writer);
    }

    private void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            writer.print(cols[i].format(data));
            if (i + 1 < cols.length)
                writer.print(", ");// delimiter
        }
        writer.println();
    }

}
