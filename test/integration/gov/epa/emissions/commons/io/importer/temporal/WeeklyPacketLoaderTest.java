package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.PacketReader;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class WeeklyPacketLoaderTest extends DbTestCase {

    private PacketReader reader;

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    private ColumnsMetadata colsMetadata;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/temporal-profiles/weekly.txt");
        colsMetadata = new TableColumnsMetadata(new WeeklyColumnsMetadata(typeMapper), typeMapper);
        reader = new PacketReader(file, colsMetadata);

        createTable("Weekly");
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "Weekly");
    }

    private void createTable(String table) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    public void testShouldLoadRecordsIntoWeeklyTable() throws Exception {
        DataLoader loader = new DataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "Weekly";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(13, tableReader.count(datasource.getName(), tableName));
    }
}
