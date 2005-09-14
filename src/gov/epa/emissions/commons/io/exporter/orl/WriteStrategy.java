package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Dataset;

import java.io.File;

public interface WriteStrategy {

    void write(Dataset dataset, File file) throws Exception;
}