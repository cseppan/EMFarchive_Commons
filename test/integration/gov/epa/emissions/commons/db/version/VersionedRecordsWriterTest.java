package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;

import java.sql.SQLException;

public class VersionedRecordsWriterTest extends VersionedRecordsTestCase {

    private VersionedRecordsWriter writer;

    protected void setUp() throws Exception {
        super.setUp();

        setupVersionZero(datasource, versionsTable);
        setupVersionZeroData(datasource, dataTable);
        writer = new VersionedRecordsWriter(datasource);
    }

    protected void tearDown() throws Exception {
        writer.close();
        super.tearDown();
    }

    private void setupVersionZero(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, createVersionsCols(), new String[] { "1", "0", "" });
    }

    private void setupVersionZeroData(Datasource datasource, String table) throws SQLException {
        DbColumn[] cols = new VersionDataColumns(types).get();

        addRecord(datasource, table, cols, new String[] { null, "1", "0", null, "p1", "p2" });// 1
        addRecord(datasource, table, cols, new String[] { null, "1", "0", null, "p21", "p22" });// 2
        addRecord(datasource, table, cols, new String[] { null, "1", "0", null, "p31", "p32" });// 3
        addRecord(datasource, table, cols, new String[] { null, "1", "0", null, "p41", "p42" });// 4
        addRecord(datasource, table, cols, new String[] { null, "1", "0", null, "p51", "p52" });// 5
    }

    public void testChangeSetWithTwoUpdatesInGivenVersion() throws Exception {
        ChangeSet changeset = new ChangeSet();

        Version baseVersion = new Version();
        baseVersion.setDatasetId(1);
        baseVersion.setVersion(0);
        baseVersion.setPath("");

        changeset.setBaseVersion(baseVersion);

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] records = reader.fetch(baseVersion);
        assertEquals(5, records.length);

        changeset.addUpdated(records[0]);
        changeset.addUpdated(records[1]);

        Version version = writer.write(changeset);

        assertEquals(1, version.getVersion());
    }

    public void testWriteChangeSetAndMarkVersionAsFinal() throws Exception {
        ChangeSet changeset = new ChangeSet();

        Version baseVersion = new Version();
        baseVersion.setDatasetId(1);
        baseVersion.setVersion(0);
        baseVersion.setPath("");

        changeset.setBaseVersion(baseVersion);

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] records = reader.fetch(baseVersion);
        assertEquals(5, records.length);

        changeset.addUpdated(records[0]);
        changeset.addUpdated(records[1]);

        Version version = writer.writeAsFinal(changeset);

        assertEquals(1, version.getVersion());
        assertTrue("Should me marked as Final", version.isFinalVersion());
    }

    public void testShouldDeleteExistingRecordAndAddNewRecordOnUpdate() throws Exception {
        // version one (based on version zero): 4 deleted, add new 6 & 7
        ChangeSet changeSetForVersionOne = new ChangeSet();

        Version versionZero = new Version();
        versionZero.setDatasetId(1);
        versionZero.setVersion(0);
        versionZero.setPath("");

        changeSetForVersionOne.setBaseVersion(versionZero);

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] versionZeroRecords = reader.fetch(versionZero);

        changeSetForVersionOne.addDeleted(versionZeroRecords[3]);// delete 4
        VersionedRecord record6 = new VersionedRecord();
        record6.setDatasetId(1);
        changeSetForVersionOne.addNew(record6);

        VersionedRecord record7 = new VersionedRecord();
        record7.setDatasetId(1);
        changeSetForVersionOne.addNew(record7);

        Version versionOne = writer.write(changeSetForVersionOne);
        assertEquals(1, versionOne.getVersion());

        // version two (based on version zero): update 3, add (new) 8
        ChangeSet changeSetForVersionTwo = new ChangeSet();
        changeSetForVersionTwo.setBaseVersion(versionZero);

        VersionedRecord record3 = versionZeroRecords[2];
        changeSetForVersionTwo.addUpdated(record3);// update 3

        VersionedRecord record8 = new VersionedRecord();
        record8.setDatasetId(1);
        changeSetForVersionTwo.addNew(record8);

        // Verify update of 3 -> delete 3, add (new)9. Verify 8 added.
        Version versionTwo = writer.write(changeSetForVersionTwo);
        assertEquals(2, versionTwo.getVersion());

        VersionedRecord[] versionTwoRecords = reader.fetch(versionTwo);
        assertEquals(6, versionTwoRecords.length);

        int start = versionTwoRecords[0].getRecordId();
        assertEquals(3 + start, versionTwoRecords[2].getRecordId());
        assertEquals("1", versionTwoRecords[2].getDeleteVersions());

        assertEquals(7 + start, versionTwoRecords[4].getRecordId());
        assertEquals("", versionTwoRecords[4].getDeleteVersions());

        assertEquals(8 + start, versionTwoRecords[5].getRecordId());
        assertEquals("", versionTwoRecords[5].getDeleteVersions());
    }

    public void testChangeSetWithAllUpdatesInGivenVersion() throws Exception {
        ChangeSet changeset = new ChangeSet();
        Version baseVersion = new Version();
        baseVersion.setDatasetId(1);
        baseVersion.setVersion(0);
        baseVersion.setPath("");

        changeset.setBaseVersion(baseVersion);

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] records = reader.fetch(baseVersion);
        assertTrue(records.length == 5);

        // update all records in the base version
        for (int i = 0; i < records.length; i++)
            changeset.addUpdated(records[i]);

        Version version = writer.write(changeset);
        assertEquals(1, version.getVersion());
    }

    public void testChangeSetWithNewRecordsResultsInNewVersion() throws Exception {
        ChangeSet changeset = new ChangeSet();

        Version baseVersion = new Version();
        baseVersion.setDatasetId(1);
        baseVersion.setVersion(0);
        baseVersion.setPath("");

        changeset.setBaseVersion(baseVersion);

        VersionedRecord record6 = new VersionedRecord();
        record6.setDatasetId(1);
        changeset.addNew(record6);

        VersionedRecord record7 = new VersionedRecord();
        record7.setDatasetId(1);
        changeset.addNew(record7);

        Version version = writer.write(changeset);
        assertNotNull("Should return version of changeset", version);
        assertEquals(1, version.getVersion());

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] records = reader.fetch(version);
        assertEquals(7, records.length);
        int init = records[0].getRecordId();
        for (int i = 1; i < records.length; i++) {
            assertEquals(++init, records[i].getRecordId());
        }
    }

    public void testChangeSetWithRecordsDeleteShouldResultInNewVersionWithoutThoseRecords() throws Exception {
        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);

        Version versionZero = new Version();
        versionZero.setDatasetId(1);
        versionZero.setVersion(0);
        versionZero.setPath("");

        ChangeSet changeset = new ChangeSet();
        changeset.setBaseVersion(versionZero);

        VersionedRecord[] records = reader.fetch(versionZero);
        changeset.addDeleted(records[1]);// delete record 2

        VersionedRecord record6 = new VersionedRecord();
        record6.setDatasetId(1);
        changeset.addNew(record6); // add record 6

        Version version = writer.write(changeset);
        assertNotNull("Should return version of changeset", version);
        assertEquals(1, version.getVersion());

        VersionedRecord[] versionOneRecords = reader.fetch(version);
        assertEquals(5, versionOneRecords.length);
        // deleted record 2
        int init = versionOneRecords[0].getRecordId();
        assertEquals(init + 2, versionOneRecords[1].getRecordId());
        assertEquals(init + 3, versionOneRecords[2].getRecordId());
        assertEquals(init + 4, versionOneRecords[3].getRecordId());
        assertEquals(init + 5, versionOneRecords[4].getRecordId());
    }

    public void testChangeSetWithAddedAndDeletedRecords() throws Exception {
        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);

        Version versionZero = new Version();
        versionZero.setDatasetId(1);
        versionZero.setVersion(0);
        versionZero.setPath("");

        ChangeSet changeset = new ChangeSet();
        changeset.setBaseVersion(versionZero);

        VersionedRecord[] records = reader.fetch(versionZero);
        changeset.addDeleted(records[1]);// record 2

        Version version = writer.write(changeset);
        assertNotNull("Should return version of changeset", version);
        assertEquals(1, version.getVersion());

        VersionedRecord[] versionOneRecords = reader.fetch(version);
        assertEquals(4, versionOneRecords.length);
        // deleted record 2
        int init = versionOneRecords[0].getRecordId();
        assertEquals(init + 2, versionOneRecords[1].getRecordId());
        assertEquals(init + 3, versionOneRecords[2].getRecordId());
        assertEquals(init + 4, versionOneRecords[3].getRecordId());
    }

}
