package gov.epa.emissions.commons.io.exporter;

import gov.epa.emissions.commons.io.Dataset;

import java.io.File;

/**
 * The exporter interface for writing a table type to a text file.
 */
public interface Exporter {

    void run(Dataset dataset, File file) throws Exception;
}
