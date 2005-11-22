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

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getDataType();

        datasource = dbServer.getEmissionsDatasource();
        versionsTable = "versions";

        setupVersionZero(datasource, versionsTable);
        setupVersionZeroData(datasource, "data");
    }

    protected void tearDown() throws Exception {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(versionsTable);
        modifier.dropAll("data");
    }

    private void setupVersionZero(Datasource datasource, String table) throws SQLException {
        addRecord(datasource, table, createVersionsCols(), new String[] { "1", "0", "" });
    }

    private void setupVersionZeroData(Datasource datasource, String table) throws SQLException {
        DbColumn[] cols = new VersionDataColumns(types).get();

        addRecord(datasource, table, cols, new String[] { "1", "1", "0", "", "p1", "p2" });
        addRecord(datasource, table, cols, new String[] { "2", "1", "0", "", "p21", "p22" });
        addRecord(datasource, table, cols, new String[] { "3", "1", "0", "", "p31", "p32" });
        addRecord(datasource, table, cols, new String[] { "4", "1", "0", "", "p41", "p42" });
        addRecord(datasource, table, cols, new String[] { "5", "1", "0", "", "p51", "p52" });
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
}
