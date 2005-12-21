package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.Dataset;

public class BaseORLImporterTest extends ORLImporterTestCase {

    //TODO: remove
    protected void doImport(String filename, Dataset dataset) throws Exception {
        //
        assertTrue(true);
        //BaseORLImporter importer = new BaseORLImporter(dbSetup.getDbServer(), true, super.types, dataset.getDatasetType(), dataset);
        //importer.setup(new File("test/data/orl/nc"), filename);
        //importer.run();
    }

}
