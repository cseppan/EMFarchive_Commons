package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DatabaseSetup;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import junit.framework.TestCase;

public abstract class DbTestCase extends TestCase {

    protected DatabaseSetup dbSetup;

    protected File fieldDefsFile;

    protected File referenceFilesDir;

    protected void setUp() throws Exception {
        String folder = "test/integration";
        File conf = new File(folder, "commons.conf");

        if (!conf.exists() || !conf.isFile()) {
            String error = "File: " + conf + " does not exist. Please copy either of the two TEMPLATE files "
                    + "(from " + folder + "), name it commons.conf, configure " + "it as needed, and rerun.";
            throw new RuntimeException(error);
        }

        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));

        properties.put("DATASET_NIF_FIELD_DEFS", "config/field_defs.dat");
        properties.put("REFERENCE_FILE_BASE_DIR", "config/refDbFiles");

        dbSetup = new DatabaseSetup(properties);
        fieldDefsFile = new File("config/field_defs.dat");
        referenceFilesDir = new File("config/refDbFiles");
    }

    protected void tearDown() throws Exception {
        dbSetup.tearDown();
    }
}