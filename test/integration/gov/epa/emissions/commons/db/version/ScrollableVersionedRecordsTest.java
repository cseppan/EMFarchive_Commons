package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbColumn;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.sql.SQLException;
import java.util.Random;

public class ScrollableVersionedRecordsTest extends PersistenceTestCase {

    private ScrollableVersionedRecords results;

    private SimpleDataset dataset;
    protected SqlDataTypes types;
    protected Datasource datasource;
    protected String dataTable;

    protected void setUp() throws Exception {
        super.setUp();

        importTestData();
        
        results = new ScrollableVersionedRecords(emissions(), "SELECT * from emissions.data");
        results.execute();
        
        
    }

    private void clean() throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.dropAll(dataTable);
    }

    private void importTestData() throws Exception {
        DbServer dbServer = dbSetup.getDbServer();
        types = dbServer.getSqlDataTypes();

        datasource = dbServer.getEmissionsDatasource();
        dataTable = "data";

        clean();
        
        DbColumn[] cols = new VersionDataColumns(types).get();

        Random rando = new Random();
        
        for (int i = 1; i <= 394; i++) {
            
            String data1 = "P1_" + i;  
            String data2 = "P2_" + i;
            addRecord(datasource, dataTable, cols, new String[] { i+"", "1", "5", "3,4", data1, data2 });            
        }

    }

    protected void addRecord(Datasource datasource, String table, DbColumn[] cols, String[] data) throws SQLException {
        DataModifier modifier = datasource.getDataModifier();
        modifier.insertRow(table, data, cols);
    }

    protected DbColumn[] createVersionsCols() {
        return new VersionsColumns(types).get();
    }

    protected void tearDown() throws Exception {
        results.close();
        clean();
        super.tearDown();
    }

    public void testRowCount() throws Exception {
        assertEquals(394, results.total());
    }

    public void testScrollForward() throws Exception {
        assertEquals(0, results.position());
        results.forward(10);
        assertEquals(10, results.position());
    }

    public void testScrollBackward() throws Exception {
        assertEquals(0, results.position());
        results.forward(10);
        results.backward(3);

        assertEquals(7, results.position());
    }

    public void testMoveToSpecificPosition() throws Exception {
        results.moveTo(3);
        assertEquals(3, results.position());
    }

    public void testFetchRangeOfRecords() throws Exception {
        VersionedRecord[] records = results.range(3, 7);
        assertNotNull("Should be able to fetch a range of records", records);
        assertEquals(5, records.length);
        for (int i = 0; i < records.length; i++)
            assertNotNull(records[i]);
    }

    public void testFetchRecordsOutOfRangeShouldReturnOnlyValidPartialRange() throws Exception {
        assertRecords(6, results.range(388, 397));
        assertRecords(93, results.range(301, 399));
        assertRecords(394, results.range(0, 399));
        assertRecords(10, results.range(0, 9));
    }

    private void assertRecords(int expected, VersionedRecord[] range) {
        assertEquals(expected, range.length);
        for (int i = 0; i < range.length; i++)
            assertNotNull(range[i]);
    }

    public void testIterate() throws Exception {
        VersionedRecord record = results.next();
        assertNotNull("Should be able to iterate through records", record);
        
        int firstIndex = record.getRecordId();
        for (int i = 1; i < 394; i++) {
            assertTrue("Should have more records", results.available());
            record = results.next();
            assertEquals(firstIndex + i, record.getRecordId());
        }
    }

    public void testFetchFirstRecord() throws Exception {
        VersionedRecord record = results.next();
        assertNotNull("Should be able to fetch first record", record);
        assertEquals(2, record.size());

        assertEquals(1, record.getRecordId());
        assertEquals(1, record.getDatasetId());
        assertEquals(5, record.getVersion());
        assertEquals("3,4", record.getDeleteVersions());
        assertEquals("P1_1", record.token(0));
        assertEquals("P2_1", record.token(1));
    }

    
    
    
}
