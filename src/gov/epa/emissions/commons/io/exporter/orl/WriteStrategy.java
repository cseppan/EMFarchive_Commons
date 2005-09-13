package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.EmfDataset;

import java.io.File;

public interface WriteStrategy {

    void write(EmfDataset dataset, File file) throws Exception;
}