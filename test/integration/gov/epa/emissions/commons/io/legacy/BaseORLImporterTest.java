package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;

public class BaseORLImporterTest extends ORLImporterTestCase {

    protected void doImport(String filename, Dataset dataset) throws Exception {
        Importer importer = new BaseORLImporter(dbSetup.getDbServer(), true, super.types, dataset.getDatasetType(), dataset);
        //FIXME: importer.setup(new File("test/data/orl/nc"));
        importer.run();
    }

}
