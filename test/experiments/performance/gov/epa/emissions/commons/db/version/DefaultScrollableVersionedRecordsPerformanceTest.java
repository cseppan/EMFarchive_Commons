package gov.epa.emissions.commons.db.version;

import gov.epa.emissions.commons.PerformanceTestCase;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

import java.util.List;

public class DefaultScrollableVersionedRecordsPerformanceTest extends PerformanceTestCase {

    private DefaultScrollableVersionedRecords results;

    protected Datasource datasource;

    protected String dataTable;

    public DefaultScrollableVersionedRecordsPerformanceTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();

        datasource = emissions();
        dataTable = "emissions.nonroad_capann_nei2002_0110_x_txt";

        String query = "SELECT * FROM emissions.nonroad_capann_nei2002_0110_x_txt "
                + "WHERE dataset_id = 2 AND version IN (0) AND  "
                + "delete_versions NOT SIMILAR TO '(0|0,%|%,0,%|%,0)' ORDER BY record_id";
        results = new DefaultScrollableVersionedRecords(emissions(), query);
        dumpMemory();
        results.execute();
        dumpMemory();
    }

    protected TableFormat tableFormat(final SqlDataTypes types) {
        FileFormatWithOptionalCols fileFormat = new FileFormatWithOptionalCols() {
            public Column[] optionalCols() {
                return new Column[0];
            }

            public Column[] minCols() {
                Column p1 = new Column("p1", types.text());
                Column p2 = new Column("p2", types.text());
                Column p3 = new Column("p3", types.intType());
                Column p4 = new Column("p4", types.realType());

                return new Column[] { p1, p2, p3, p4 };
            }

            public String identify() {
                return "Record_Id";
            }

            public Column[] cols() {
                return minCols();
            }

            public void fillDefaults(List data, long datasetId) {// ignore
            }
        };
        return new VersionedTableFormat(fileFormat, types);
    }

    protected void doTearDown() throws Exception {
        results.close();
    }

    public void testRowCount() throws Exception {
        assertEquals(60780, results.total());
    }

   

    public void testFetchRangeOfRecords() throws Exception {
        VersionedRecord[] records = results.range(1, 100);
        assertNotNull("Should be able to fetch a range of records", records);
        assertEquals(100, records.length);
        for (int i = 0; i < records.length; i++)
            assertNotNull(records[i]);
    }

}
