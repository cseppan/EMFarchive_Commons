package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.legacy.BaseORLImporter;

import java.io.File;

public class BaseORLImporterTest extends ORLImporterTestCase {

    protected void doImport(String filename, Dataset dataset) throws Exception {
        Importer importer = new BaseORLImporter(dbSetup.getDbServer(), true, super.types, dataset.getDatasetType());
        importer.preCondition(new File("test/data/orl/nc"), filename);
        importer.run(dataset);
    }

}
