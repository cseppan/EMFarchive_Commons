package gov.epa.emissions.commons.io;

import java.io.File;

public interface Exporter {

    void export(File file) throws ExporterException;

    void export(int version, File file) throws ExporterException;

}
