package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.VersionsColumns;
import gov.epa.emissions.commons.db.version.Versions;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;

public class VersionsTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes types;

    private String versionsTable;

    private Versions versions;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";

        setupData(datasource, versionsTable);

        versions = new Versions(datasource);
    }

    protected void tearDown() throws Exception {
        versions.close();

        DataModifier modifier = datasource.dataModifier();
        modifier.dropAll(versionsTable);
    }

    private void setupData(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, createVersionsCols(), new String[] { "1", "0", "" });
    }

    private void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.insertRow(table, data, cols);
    }

    private DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

    public void testFetchVersionZero() throws Exception {
        Version[] path = versions.getPath(1, 0);

        assertEquals(1, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[0].getDatasetId());
    }

    public void testShouldDeriveVersionFromAFinalVersion() throws Exception {
        Version base = versions.get(1, 0);

        Version derived = versions.derive(base);

        assertNotNull("Should be able to derive from a Final version", derived);
        assertEquals(1, derived.getDatasetId());
        assertEquals(1, derived.getVersion());
        assertEquals("0", derived.getPath());
        assertFalse("Derived version should be non-final", derived.isFinalVersion());
    }

    public void testShouldBeAbleToMarkADerivedVersionAsFinal() throws Exception {
        Version base = versions.get(1, 0);
        Version derived = versions.derive(base);

        Version finalVersion = versions.markFinal(derived);

        assertNotNull("Should be able to mark a 'derived' as a Final version", derived);
        assertEquals(derived.getDatasetId(), finalVersion.getDatasetId());
        assertEquals(derived.getVersion(), finalVersion.getVersion());
        assertEquals("0", finalVersion.getPath());
        assertTrue("Derived version should be final on being marked 'final'", finalVersion.isFinalVersion());
        
        Version results = versions.get(1, derived.getVersion());
        assertEquals(derived.getDatasetId(), results.getDatasetId());
        assertEquals(derived.getVersion(), results.getVersion());
        assertEquals(derived.getPath(), results.getPath());
        assertTrue("Derived version should be marked final in db", results.isFinalVersion());
    }

    public void testNonLinearVersionFourShouldHaveZeroAndOneInThePath() throws Exception {
        DbColumn[] cols = createVersionsCols();

        String[] versionOneData = { "1", "1", "0" };
        addRecord(datasource, versionsTable, cols, versionOneData);

        String[] versionFourData = { "1", "4", "0,1" };
        addRecord(datasource, versionsTable, cols, versionFourData);

        Version[] path = versions.getPath(1, 4);

        assertEquals(3, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[1].getVersion());
        assertEquals(4, path[2].getVersion());
    }

    public void testLinearVersionThreeShouldHaveZeroOneAndTwoInThePath() throws Exception {
        DbColumn[] cols = createVersionsCols();

        String[] versionOneData = { "1", "1", "0" };
        addRecord(datasource, versionsTable, cols, versionOneData);

        String[] versionTwoData = { "1", "2", "0,1" };
        addRecord(datasource, versionsTable, cols, versionTwoData);

        String[] versionThreeData = { "1", "3", "0,1,2" };
        addRecord(datasource, versionsTable, cols, versionThreeData);

        Version[] path = versions.getPath(1, 3);

        assertEquals(4, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[1].getVersion());
        assertEquals(2, path[2].getVersion());
        assertEquals(3, path[3].getVersion());
    }
}
