package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.exporter.orl.ExporterException;

import java.io.File;

public interface NewExporter {

    void export(File file) throws ExporterException;

}
