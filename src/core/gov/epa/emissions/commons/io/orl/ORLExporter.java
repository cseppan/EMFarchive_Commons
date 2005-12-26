package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleExportStatement;

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

    private ExportStatement exportStatement;

    public ORLExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat, ExportStatement exportStatement) {
        this.dataset = dataset;
        this.datasource = datasource;
        this.fileFormat = fileFormat;
        this.exportStatement = exportStatement;
    }

    public ORLExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        this(dataset, datasource, fileFormat, new SimpleExportStatement());
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
        writer.println(dataset.getDescription());
    }

    private void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        InternalSource source = dataset.getInternalSources()[0];

        String qualifiedTable = datasource.getName() + "." + source.getTable();
        DataQuery q = datasource.query();
        ResultSet data = q.executeQuery(exportStatement.generate(qualifiedTable));
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
