package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public interface NewImporter {

    void run(File file, Dataset dataset) throws ImporterException;

}