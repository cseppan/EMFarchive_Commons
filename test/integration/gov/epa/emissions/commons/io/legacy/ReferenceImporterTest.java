package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.legacy.ReferenceImporter;

public class ReferenceImporterTest extends PersistenceTestCase {
    
    public void testImportReference() throws Exception {
        ReferenceImporter referenceImporter = new ReferenceImporter(dbSetup.getDbServer(), fieldDefsFile,
                referenceFilesDir, false, null, null);
        referenceImporter.run();
    }

}
