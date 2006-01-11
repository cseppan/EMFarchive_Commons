package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.HibernateTestCase;

import java.sql.SQLException;
import java.util.Date;

public class VersionsTest extends HibernateTestCase {

    private Datasource datasource;

    private String versionsTable;

    private Versions versions;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";

        setupData(datasource, versionsTable);

        versions = new Versions();
    }

    protected void doTearDown() throws Exception {
        dropData(versionsTable, datasource);
    }

    private void setupData(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, new String[] { null, "1", "0", "version zero", "", "true" });
    }

    private void addRecord(Datasource datasource, String table, String[] data) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.insertRow(table, data);
    }

    public void testFetchVersionZero() throws Exception {
        Version[] path = versions.getPath(1, 0, session);

        assertEquals(1, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[0].getDatasetId());
    }

    public void testGetsLastFinalVersionForDataset() throws Exception {
        int finalVersion = versions.getLastFinalVersion(1, session);
        assertEquals(0, finalVersion);
    }

    public void testSeveralMarksDerivedVersionAsFinal() throws Exception {
        long datasetId = 1;
        Version base = versions.get(datasetId, 0, session);

        Version derivedFromBase = versions.derive(base, "version one", session);
        versions.markFinal(derivedFromBase, session);
        int versionOne = versions.getLastFinalVersion(datasetId, session);
        assertEquals(derivedFromBase.getVersion(), versionOne);

        Version derivedFromOne = versions.derive(derivedFromBase, "version two", session);
        versions.markFinal(derivedFromOne, session);
        int versionTwo = versions.getLastFinalVersion(datasetId, session);
        assertEquals(derivedFromOne.getVersion(), versionTwo);
    }

    public void testShouldDeriveVersionFromAFinalVersion() throws Exception {
        Version base = versions.get(1, 0, session);

        Version derived = versions.derive(base, "version one", session);

        assertNotNull("Should be able to derive from a Final version", derived);
        assertEquals(1, derived.getDatasetId());
        assertEquals(1, derived.getVersion());
        assertEquals("0", derived.getPath());
        assertFalse("Derived version should be non-final", derived.isFinalVersion());
        assertNotNull(derived.getDate());
    }

    public void testShouldGetAVersionUsingHibernate() throws Exception {
        Version base = versions.get(1, 0, session);

        assertEquals(1, base.getDatasetId());
        assertEquals(0, base.getVersion());
        assertEquals("", base.getPath());
        assertTrue("Version zero should be final", base.isFinalVersion());
        assertNotNull(base.getDate());
    }

    public void testShouldGetAllVersionsBasedOnADerivedVersion() throws Exception {
        Version base = versions.get(1, 0, session);
        Version derived = versions.derive(base, "version one", session);

        Version[] allVersions = versions.get(1, session);

        assertNotNull("Should get all versions of a Dataset", allVersions);
        assertEquals(2, allVersions.length);
        assertEquals(base.getVersion(), allVersions[0].getVersion());
        assertEquals(derived.getVersion(), allVersions[1].getVersion());
        assertEquals(1, allVersions[1].getVersion());
        assertEquals("version one", allVersions[1].getName());
    }

    public void testShouldGetAllVersions() throws Exception {
        Version[] allVersions = versions.get(1, session);

        assertNotNull("Should get all versions of a Dataset", allVersions);
        assertEquals(1, allVersions.length);
        assertEquals(0, allVersions[0].getVersion());
    }

    public void testShouldFailWhenTryingToDeriveVersionFromANonFinalVersion() throws Exception {
        Version base = versions.get(1, 0, session);

        Version derived = versions.derive(base, "version one", session);
        try {
            versions.derive(derived, "version two", session);
        } catch (Exception e) {
            return;
        }

        fail("Should failed to derive from a non-final version");
    }

    public void testShouldBeAbleToMarkADerivedVersionAsFinal() throws Exception {
        Version base = versions.get(1, 0, session);
        Version derived = versions.derive(base, "version one", session);
        Date creationDate = derived.getDate();

        Version finalVersion = versions.markFinal(derived, session);

        assertNotNull("Should be able to mark a 'derived' as a Final version", derived);
        assertEquals(derived.getDatasetId(), finalVersion.getDatasetId());
        assertEquals(derived.getVersion(), finalVersion.getVersion());
        assertEquals("0", finalVersion.getPath());
        assertTrue("Derived version should be final on being marked 'final'", finalVersion.isFinalVersion());

        Version results = versions.get(1, derived.getVersion(), session);
        assertEquals(derived.getDatasetId(), results.getDatasetId());
        assertEquals(derived.getVersion(), results.getVersion());
        assertEquals(derived.getPath(), results.getPath());
        assertTrue("Derived version should be marked final in db", results.isFinalVersion());

        Date finalDate = results.getDate();
        assertTrue("Creation Date should be different from Final Date", !finalDate.before(creationDate));
        
        Version[] all = versions.get(1, session);
        assertEquals(2, all.length);
    }

    public void testNonLinearVersionFourShouldHaveZeroAndOneInThePath() throws Exception {
        String[] versionOneData = { "2", "1", "1", "ver 1", "0" };
        addRecord(datasource, versionsTable, versionOneData);

        String[] versionFourData = { "3", "1", "4", "ver 4", "0,1" };
        addRecord(datasource, versionsTable, versionFourData);

        Version[] path = versions.getPath(1, 4, session);

        assertEquals(3, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[1].getVersion());
        assertEquals(4, path[2].getVersion());
    }

    public void testLinearVersionThreeShouldHaveZeroOneAndTwoInThePath() throws Exception {
        String[] versionOneData = { "2", "1", "1", "ver 1", "0" };
        addRecord(datasource, versionsTable, versionOneData);

        String[] versionTwoData = { "3", "1", "2", "ver 2", "0,1" };
        addRecord(datasource, versionsTable, versionTwoData);

        String[] versionThreeData = { "4", "1", "3", "ver 3", "0,1,2" };
        addRecord(datasource, versionsTable, versionThreeData);

        Version[] path = versions.getPath(1, 3, session);

        assertEquals(4, path.length);
        assertEquals(0, path[0].getVersion());
        assertEquals(1, path[1].getVersion());
        assertEquals(2, path[2].getVersion());
        assertEquals(3, path[3].getVersion());
    }
}
