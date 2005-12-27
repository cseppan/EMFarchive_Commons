package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.orl.ORLImporter;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//FIXME: move the functionality to appropriate components/classes. Remove this class.
public class HelpImporter_REMOVE_ME {

    private static Log log = LogFactory.getLog(ORLImporter.class);

    public void validateFile(File file) throws ImporterException {
        log.debug("check if file exists " + file.getName());
        if (!file.exists()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new ImporterException("The file '" + file + "' does not exist");
        }

        if (!file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " is not a file");
            throw new ImporterException("The file '" + file + "' is not a file");
        }
    }

}
