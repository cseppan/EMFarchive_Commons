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
        addRecord(datasource, table, cols, new String[] { "2", "1", "0", null, "p21", "p22" });
        addRecord(datasource, table, cols, new String[] { "3", "1", "0", null, "p31", "p32" });
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

    public void xtestFetchVersionZero() throws Exception {
        Version versionZero = new Version();
        versionZero.setDatasetId(1);
        versionZero.setVersion(0);

        VersionedDataReader reader = new VersionedDataReader(datasource);

        VersionedRecord[] records = reader.fetch(versionZero);

        assertEquals(5, records.length);

        assertEquals(0, records[0].getVersion());
        assertEquals(1, records[0].getDatasetId());
    }

    public void testFetchVersionOneThatHasARecordDeleteFromVersionZero() throws Exception {
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

        assertEquals(6, records.length);

        assertEquals(0, records[0].getVersion());
        assertEquals(1, records[0].getRecordId());

        assertEquals(0, records[1].getVersion());
        assertEquals(2, records[1].getRecordId());
        
        assertEquals(0, records[2].getVersion());
        assertEquals(3, records[2].getRecordId());
        
        assertEquals(0, records[3].getVersion());
        assertEquals(4, records[3].getRecordId());
        
        assertEquals(0, records[4].getVersion());
        assertEquals(5, records[4].getRecordId());
        
        assertEquals(1, records[5].getVersion());
        assertEquals(7, records[5].getRecordId());
        
    }
}
