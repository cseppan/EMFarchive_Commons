package gov.epa.emissions.commons.io.ref;

import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.ref.ReferenceTablesCreator;

public class ReferenceTablesCreatorTest extends PersistenceTestCase {

    public void testCreateAddtionalTables() throws Exception {
        ReferenceTablesCreator tables = new ReferenceTablesCreator(referenceFilesDir, dbSetup.getDbServer().getSqlDataTypes());
        tables.createAdditionalRefTables(dbSetup.getDbServer().getReferenceDatasource());
    }

}
