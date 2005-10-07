package gov.epa.emissions.commons.io.importer.temporal;

import java.io.File;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlTypeMapper;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

public class MonthlyPacketLoaderTest extends DbTestCase {

    private PacketReader reader;

    private Datasource datasource;

    private SqlTypeMapper typeMapper;

    protected void setUp() throws Exception {
        super.setUp();

        File file = new File("test/data/temporal-profiles/monthly.txt");
        reader = new PacketReader(file);

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getTypeMapper();
        datasource = dbServer.getEmissionsDatasource();
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "monthly");
    }

    public void testShouldLoadRecordsIntoMonthlyTable() throws Exception {
        MonthlyPacketLoader loader = new MonthlyPacketLoader(datasource, typeMapper);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        loader.load(dataset, reader);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        String tableName = "monthly";

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(), tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}
