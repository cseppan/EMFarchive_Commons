package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Dataset;

import java.io.File;

public interface NewImporter {

    void run(File file, Dataset dataset) throws ImporterException;

}