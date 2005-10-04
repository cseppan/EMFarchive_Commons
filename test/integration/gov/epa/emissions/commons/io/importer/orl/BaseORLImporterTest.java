package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;

import java.io.File;

public class BaseORLImporterTest extends ORLImporterTestCase {

    protected void doImport(String filename, Dataset dataset) throws Exception {
        Importer importer = new BaseORLImporter(dbSetup.getDbServer(), true);
        importer.run(new File[] { new File("test/data/orl/nc", filename) }, dataset, true);
    }

}
