package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;

public class WeeklyPacketLoaderTest extends DbTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    private TableColumnsMetadata colsMetadata;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        colsMetadata = new TableColumnsMetadata(new WeeklyColumnsMetadata(typeMapper), typeMapper);
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
        File file = new File("test/data/temporal-profiles/weekly.txt");
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        reader = new FixedWidthPacketReader(fileReader, fileReader.readLine().trim(), colsMetadata);

        try {
            DataLoader loader = new DataLoader(datasource, colsMetadata);

            Dataset dataset = new SimpleDataset();
            dataset.setName("test");
            String tableName = "Weekly";

            loader.load(reader, dataset, tableName);

            // assert
            assertEquals(13, countRecords(tableName));
        } finally {
            fileReader.close();
        }
    }

    private int countRecords(String tableName) {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), tableName);
    }

    public void testShouldDropDataOnEncounteringBadData() throws Exception {
        File file = new File("test/data/temporal-profiles/BAD-weekly.txt");
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        reader = new FixedWidthPacketReader(fileReader, fileReader.readLine().trim(), colsMetadata);

        DataLoader loader = new DataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "Weekly";

        try {
            loader.load(reader, dataset, tableName);
        } catch (ImporterException e) {
            assertEquals(0, countRecords(tableName));
            return;
        } finally {
            fileReader.close();
        }

        fail("should have encountered an error(missing cols) on record 3");
    }
}
