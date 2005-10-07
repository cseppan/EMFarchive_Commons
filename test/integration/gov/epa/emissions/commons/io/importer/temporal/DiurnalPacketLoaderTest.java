package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataType;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class DiurnalPacketLoaderTest extends DbTestCase {

    private PacketReader reader;

    private Datasource datasource;

    private SqlDataType typeMapper;

    private ColumnsMetadata colsMetadata;

    private PacketLoader loader;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getTypeMapper();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/temporal-profiles/diurnal-weekday.txt");
        colsMetadata = new TableColumnsMetadata(new DiurnalColumnsMetadata(typeMapper), typeMapper);
        reader = new PacketReader(file, colsMetadata);

        createTable("Diurnal_Weekday");

        loader = new PacketLoader(datasource, colsMetadata);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekday");
    }

    private void createTable(String table) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    public void testShouldLoadRecordsIntoWeeklyTable() throws Exception {
        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        loader.load(dataset, reader);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        String tableName = "Diurnal_Weekday";

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(20, tableReader.count(datasource.getName(), tableName));
    }
}
