package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Dataset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class NewORLExporter {

    private Dataset dataset;

    public NewORLExporter(Dataset dataset) {
        this.dataset = dataset;
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
        } finally {
            writer.close();
        }
    }

    private void writeHeaders(PrintWriter writer, Dataset dataset) {
        writer.println(dataset.getDescription());
    }

}
