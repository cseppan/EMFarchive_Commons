package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class GenericExporter implements Exporter {
    private Dataset dataset;

    private Datasource datasource;

    private FileFormat fileFormat;

    private String delimiter;

    private boolean formatted;

    private DataFormatFactory dataFormatFactory;

    public GenericExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat) {
        this(dataset, dbServer, fileFormat, new NonVersionedDataFormatFactory());
    }

    public GenericExporter(Dataset dataset, DbServer dbServer, FileFormat fileFormat,
            DataFormatFactory dataFormatFactory) {
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
        this.fileFormat = fileFormat;
        this.dataFormatFactory = dataFormatFactory;

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
        String header = dataset.getDescription();

        if (header != null) {
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()) {
                writer.print("#" + st.nextToken());
            }
        }
    }

    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = dataset.getInternalSources()[0];

        String qualifiedTable = datasource.getName() + "." + source.getTable();
        ExportStatement export = dataFormatFactory.exportStatement();
        ResultSet data = q.executeQuery(export.generate(qualifiedTable));
        Column[] cols = fileFormat.cols();
        while (data.next())
            writeRecord(cols, data, writer);
    }

    protected void writeRecord(Column[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        for (int i = 0; i < cols.length; i++) {
            if (formatted)
                writer.print(cols[i].format(data));
            else
                writer.print(cols[i].format(data).trim());

            if (i + 1 < cols.length)
                writer.print(delimiter);// delimiter
        }
        writer.println();
    }

    public void setDelimiter(String del) {
        this.delimiter = del;
    }

    public void setFormatted(boolean formatted) {
        this.formatted = formatted;
    }
}
