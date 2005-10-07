package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlTypeMapper;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class MonthlyPacketLoaderTest extends DbTestCase {

    private PacketReader reader;

    private Datasource datasource;

    private SqlTypeMapper typeMapper;

    private ColumnsMetadata colsMetadata;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getTypeMapper();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/temporal-profiles/monthly.txt");
        colsMetadata = new TableColumnsMetadata(new MonthlyColumnsMetadata(typeMapper), typeMapper);
        reader = new PacketReader(file, colsMetadata);

        createTable("Monthly");
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "monthly");
    }

    private void createTable(String table) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    public void testShouldLoadRecordsIntoMonthlyTable() throws Exception {
        PacketLoader loader = new PacketLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        loader.load(dataset, reader);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        String tableName = "monthly";

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}
