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

    public void testChangeSetWithNewRecordsResultsInNewVersion() throws Exception {
        ChangeSet changeset = new ChangeSet();

        Version baseVersion = new Version();
        baseVersion.setDatasetId(1);
        baseVersion.setVersion(0);
        baseVersion.setParentVersions("");

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
        versionZero.setParentVersions("");

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
