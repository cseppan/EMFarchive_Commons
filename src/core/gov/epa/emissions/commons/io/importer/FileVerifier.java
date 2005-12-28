package gov.epa.emissions.commons.io.importer;

import java.io.File;

public class FileVerifier {

    public void exists(File file) throws ImporterException {
        if (!file.exists()) {
            throw new ImporterException("The file '" + file + "' does not exist");
        }

        if (!file.isFile()) {
            throw new ImporterException("The file '" + file + "' is not a file");
        }
    }

}
