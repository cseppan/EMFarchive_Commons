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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;

public class DiurnalPacketLoaderTest extends DbTestCase {

    private PacketReader reader;

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    private ColumnsMetadata colsMetadata;

    private DataLoader loader;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();
        colsMetadata = new TableColumnsMetadata(new DiurnalColumnsMetadata(typeMapper), typeMapper);

        createTable("Diurnal_Weekday");
        loader = new DataLoader(datasource, colsMetadata);

        File file = new File("test/data/temporal-profiles/diurnal-weekday.txt");
        fileReader = new BufferedReader(new FileReader(file));
        reader = new PacketReader(fileReader, fileReader.readLine().trim(), colsMetadata);
    }

    protected void tearDown() throws Exception {
        fileReader.close();
        
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
        String tableName = "Diurnal_Weekday";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(20, tableReader.count(datasource.getName(), tableName));
    }
}
