package gov.epa.emissions.commons.io.ref;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.reference.ReferenceCVSFileImporter;

import java.io.File;

public class ReferenceCVSFileImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private String tableName;

    protected void setUp() throws Exception {
        super.setUp();
        tableName = "test";
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), tableName);
    }

    public void testShouldImportASmallAndSimplePointFile() throws Exception {
        File file = new File("test/data/reference", "pollutants.txt");

        Importer importer = new ReferenceCVSFileImporter(file, tableName, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(8, rows);
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }

}
