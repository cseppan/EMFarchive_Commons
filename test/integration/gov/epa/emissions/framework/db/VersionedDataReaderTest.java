package gov.epa.emissions.framework.db;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;

public class VersionedDataReaderTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes types;

    private String versionsTable;

    private String dataTable;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getDataType();

        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";
        dataTable = "data";

        setupVersionZero(datasource, versionsTable);
        setupVersionZeroData(datasource, dataTable);
    }

    protected void tearDown() throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(versionsTable);
        modifier.dropAll(dataTable);
    }

    private void setupVersionZero(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, createVersionsCols(), new String[] { "1", "0", "" });
    }

    private void setupVersionZeroData(Datasource datasource, String table) throws SQLException {
        DbColumn[] cols = new VersionDataColumns(types).get();

        addRecord(datasource, table, cols, new String[] { "1", "1", "0", null, "p1", "p2" });
        addRecord(datasource, table, cols, new String[] { "2", "1", "0", "6", "p21", "p22" });
        addRecord(datasource, table, cols, new String[] { "3", "1", "0", "2", "p31", "p32" });
        addRecord(datasource, table, cols, new String[] { "4", "1", "0", null, "p41", "p42" });
        addRecord(datasource, table, cols, new String[] { "5", "1", "0", null, "p51", "p52" });
    }

    private void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.insertRow(table, data, cols);
    }

    private DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

    public void testFetchVersionZero() throws Exception {
        Version versionZero = new Version();
        versionZero.setDatasetId(1);
        versionZero.setVersion(0);

        VersionedDataReader reader = new VersionedDataReader(datasource);

        VersionedRecord[] records = reader.fetch(versionZero);

        assertEquals(5, records.length);

        assertEquals(0, records[0].getVersion());
        assertEquals(1, records[0].getDatasetId());
    }

    public void testFetchVersionTwoThatHasARecordDeleteFromVersionOne() throws Exception {
        DbColumn[] cols = new VersionDataColumns(types).get();

        // mark record 6 as deleted from version 2
        addRecord(datasource, dataTable, cols, new String[] { "6", "1", "1", "2", "p61", "p62" });
        addRecord(datasource, dataTable, cols, new String[] { "7", "1", "1", null, "p71", "p72" });
        // setup version sequence
        addRecord(datasource, versionsTable, createVersionsCols(), new String[] { "1", "1", "0" });
        addRecord(datasource, versionsTable, createVersionsCols(), new String[] { "1", "2", "0,1" });

        Version versionTwo = new Version();
        versionTwo.setDatasetId(1);
        versionTwo.setVersion(2);

        VersionedDataReader reader = new VersionedDataReader(datasource);
        VersionedRecord[] records = reader.fetch(versionTwo);

        assertEquals(5, records.length);

        assertEquals(1, records[0].getRecordId());
        assertEquals(2, records[1].getRecordId());
        assertEquals(4, records[2].getRecordId());
        assertEquals(5, records[3].getRecordId());
        assertEquals(7, records[4].getRecordId());
    }

    public void testFetchWithDeletesAcrossMultipleVersions() throws Exception {
        // mark record 6 as deleted from version 2
        DbColumn[] cols = new VersionDataColumns(types).get();
        addRecord(datasource, dataTable, cols, new String[] { "6", "1", "1", "2", "p61", "p62" });
        addRecord(datasource, dataTable, cols, new String[] { "7", "1", "1", null, "p71", "p72" });
        addRecord(datasource, dataTable, cols, new String[] { "8", "1", "2", "3", "p81", "p82" });
        addRecord(datasource, dataTable, cols, new String[] { "9", "1", "3", null, "p1", "p2" });
        addRecord(datasource, dataTable, cols, new String[] { "10", "1", "4", null, "p", "p2" });
        addRecord(datasource, dataTable, cols, new String[] { "11", "1", "4", "5", "p", "p2" });
        addRecord(datasource, dataTable, cols, new String[] { "12", "1", "5", null, "p", "p2" });
        addRecord(datasource, dataTable, cols, new String[] { "13", "1", "5", null, "p", "p2" });
        addRecord(datasource, dataTable, cols, new String[] { "14", "1", "6", null, "p", "p2" });

        // setup version sequence
        DbColumn[] versionCols = createVersionsCols();
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "1", "0" });
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "2", "0,1" });
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "3", "0,1,2" });
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "4", "0,1" });
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "5", "0,1,4" });
        addRecord(datasource, versionsTable, versionCols, new String[] { "1", "6", "0,1" });

        verifyVersionThree();
        verifyVersionFive();
        verifyVersionTwo();
    }

    private void verifyVersionThree() throws SQLException {
        Version version = new Version();
        version.setDatasetId(1);
        version.setVersion(3);

        VersionedDataReader reader = new VersionedDataReader(datasource);
        VersionedRecord[] records = reader.fetch(version);

        assertEquals(6, records.length);

        assertEquals(1, records[0].getRecordId());
        assertEquals(2, records[1].getRecordId());
        assertEquals(4, records[2].getRecordId());
        assertEquals(5, records[3].getRecordId());
        assertEquals(7, records[4].getRecordId());
        assertEquals(9, records[5].getRecordId());
    }
    
    private void verifyVersionFive() throws SQLException {
        Version version = new Version();
        version.setDatasetId(1);
        version.setVersion(5);
        
        VersionedDataReader reader = new VersionedDataReader(datasource);
        VersionedRecord[] records = reader.fetch(version);
        
        assertEquals(10, records.length);
        
        assertEquals(1, records[0].getRecordId());
        assertEquals(2, records[1].getRecordId());
        assertEquals(3, records[2].getRecordId());
        assertEquals(4, records[3].getRecordId());
        assertEquals(5, records[4].getRecordId());
        assertEquals(6, records[5].getRecordId());
        assertEquals(7, records[6].getRecordId());
        assertEquals(10, records[7].getRecordId());
        assertEquals(12, records[8].getRecordId());
        assertEquals(13, records[9].getRecordId());
    }
    
    private void verifyVersionTwo() throws SQLException {
        Version version = new Version();
        version.setDatasetId(1);
        version.setVersion(2);
        
        VersionedDataReader reader = new VersionedDataReader(datasource);
        VersionedRecord[] records = reader.fetch(version);
        
        assertEquals(6, records.length);
        
        assertEquals(1, records[0].getRecordId());
        assertEquals(2, records[1].getRecordId());
        assertEquals(4, records[2].getRecordId());
        assertEquals(5, records[3].getRecordId());
        assertEquals(7, records[4].getRecordId());
        assertEquals(8, records[5].getRecordId());
    }
}
