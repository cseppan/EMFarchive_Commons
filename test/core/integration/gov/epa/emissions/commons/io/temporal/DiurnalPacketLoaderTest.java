package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.FixedWidthPacketReader;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class DiurnalPacketLoaderTest extends PersistenceTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    private FixedColsTableFormat tableFormat;

    private DataLoader loader;

    private BufferedReader fileReader;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
        DiurnalFileFormat fileFormat = new DiurnalFileFormat(typeMapper);
        tableFormat = new FixedColsTableFormat(fileFormat, typeMapper);

        createTable("Diurnal_Weekday", datasource, tableFormat);
        loader = new FixedColumnsDataLoader(datasource, tableFormat);

        File file = new File("test/data/temporal-profiles/diurnal-weekday.txt");
        fileReader = new BufferedReader(new FileReader(file));
        reader = new FixedWidthPacketReader(fileReader, fileReader.readLine().trim(), fileFormat);
    }

    protected void doTearDown() throws Exception {
        fileReader.close();

        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekday");
    }

    public void testShouldLoadRecordsIntoWeeklyTable() throws Exception {
        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "Diurnal_Weekday";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = tableReader(datasource);

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(20, tableReader.count(datasource.getName(), tableName));
    }
}
