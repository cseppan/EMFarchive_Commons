package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.MonthlyFileFormat;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class MonthlyPacketLoaderTest extends DbTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    public FixedColsTableFormat tableColsMetadata;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/temporal-profiles/monthly.txt");
        MonthlyFileFormat colsMetadata = new MonthlyFileFormat(typeMapper);
        tableColsMetadata = new FixedColsTableFormat(colsMetadata, typeMapper);
        createTable("Monthly", datasource, tableColsMetadata);
        
        fileReader = new BufferedReader(new FileReader(file));
        reader = new FixedWidthPacketReader(fileReader, fileReader.readLine().trim(), colsMetadata);
    }

    protected void tearDown() throws Exception {
        fileReader.close();
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "monthly");
    }

    public void testShouldLoadRecordsIntoMonthlyTable() throws Exception {
        DataLoader loader = new FixedColumnsDataLoader(datasource, tableColsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "monthly";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}