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
        changeset.add(record6);

        VersionedRecord record7 = new VersionedRecord();
        record7.setDatasetId(1);
        changeset.add(record7);

        Version version = writer.write(changeset);
        assertNotNull("Should return version of changeset", version);
        assertEquals(1, version.getVersion());

        VersionedRecordsReader reader = new VersionedRecordsReader(datasource);
        VersionedRecord[] records = reader.fetch(version);
        assertEquals(7, records.length);
    }

}
