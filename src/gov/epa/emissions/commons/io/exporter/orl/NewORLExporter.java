package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

public class NewORLExporter {

    private Dataset dataset;

    private Datasource datasource;

    private ORLColumnsMetadata colsMetadata;

    public NewORLExporter(Dataset dataset, Datasource datasource, ORLColumnsMetadata colsMetadata) {
        this.dataset = dataset;
        this.datasource = datasource;
        this.colsMetadata = colsMetadata;
    }

    public void export(File file) throws ExporterException {
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        } catch (IOException e) {
            throw new ExporterException("could not open file - " + file + " for writing");
        }

        try {
            writeHeaders(writer, dataset);
            writerData(writer, dataset, datasource);
        } catch (SQLException e) {
            throw new ExporterException("could not export file - " + file, e);
        } finally {
            writer.close();
        }
    }

    private void writerData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = (InternalSource) dataset.getInternalSources().get(0);

        ResultSet data = q.selectAll(source.getTable());
        Column[] cols = colsMetadata.cols();
        while (data.next())
            writeRecord(cols, data, writer);
    }

    private void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            cols[i].format(data, writer);
        }
        writer.println();
    }

    private void writeHeaders(PrintWriter writer, Dataset dataset) {
        writer.println(dataset.getDescription());
    }

}
