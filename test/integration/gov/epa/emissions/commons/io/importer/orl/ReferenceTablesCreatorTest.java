package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ref.ReferenceTablesCreator;

public class ReferenceTablesCreatorTest extends DbTestCase {

    public void testCreateAddtionalTablesUsingMysql() throws Exception {
        ReferenceTablesCreator tables = new ReferenceTablesCreator(referenceFilesDir, dbSetup.getDbServer().getTypeMapper());
        tables.createAdditionalRefTables(dbSetup.getDbServer().getReferenceDatasource());
    }

}
