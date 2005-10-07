package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ref.ReferenceImporter;

public class ReferenceImporterTest extends DbTestCase {

    public void testImportReference() throws Exception {
        ReferenceImporter referenceImporter = new ReferenceImporter(dbSetup.getDbServer(), fieldDefsFile,
                referenceFilesDir, false);
        referenceImporter.run();
    }

}
