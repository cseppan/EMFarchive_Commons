package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.VersionsColumns;
import gov.epa.emissions.commons.db.version.VersionsReader;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;

public class VersionsReaderTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes types;

    private String versionsTable;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";
        
        setupData(datasource, versionsTable);
    }

    protected void tearDown() throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(versionsTable);
    }

    private void setupData(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, createVersionsCols(), new String[] { "1", "0", "" });
    }

    private void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.insertRow(table, data, cols);
    }

    private DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

    public void testFetchVersionZero() throws Exception {
        VersionsReader reader = new VersionsReader(datasource);

        Version[] versions = reader.fetchSequence(1, 0);

        assertEquals(1, versions.length);
        assertEquals(0, versions[0].getVersion());
        assertEquals(1, versions[0].getDatasetId());
    }

    public void testNonLinearVersionFourShouldHaveZeroAndOneInThePath() throws Exception {
        DbColumn[] cols = createVersionsCols();

        String[] versionOneData = { "1", "1", "0" };
        addRecord(datasource, versionsTable, cols, versionOneData);

        String[] versionFourData = { "1", "4", "0,1" };
        addRecord(datasource, versionsTable, cols, versionFourData);

        VersionsReader reader = new VersionsReader(datasource);

        Version[] versions = reader.fetchSequence(1, 4);

        assertEquals(3, versions.length);
        assertEquals(0, versions[0].getVersion());
        assertEquals(1, versions[1].getVersion());
        assertEquals(4, versions[2].getVersion());
    }

    public void testLinearVersionThreeShouldHaveZeroOneAndTwoInThePath() throws Exception {
        DbColumn[] cols = createVersionsCols();

        String[] versionOneData = { "1", "1", "0" };
        addRecord(datasource, versionsTable, cols, versionOneData);

        String[] versionTwoData = { "1", "2", "0,1" };
        addRecord(datasource, versionsTable, cols, versionTwoData);

        String[] versionThreeData = { "1", "3", "0,1,2" };
        addRecord(datasource, versionsTable, cols, versionThreeData);

        VersionsReader reader = new VersionsReader(datasource);

        Version[] versions = reader.fetchSequence(1, 3);

        assertEquals(4, versions.length);
        assertEquals(0, versions[0].getVersion());
        assertEquals(1, versions[1].getVersion());
        assertEquals(2, versions[2].getVersion());
        assertEquals(3, versions[3].getVersion());
    }
}
