package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;

import java.io.File;

public class DelimitedFileLoaderTest extends PersistenceTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes dataType;

    private FixedColsTableFormat colsMetadata;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        dataType = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/orl/SimpleDelimited.txt");
        reader = new DelimitedFileReader(file, new WhitespaceDelimitedTokenizer());

        colsMetadata = new FixedColsTableFormat(new DelimitedFileFormat("test", 7, dataType), dataType);
        createTable("SimpleDelimited", datasource, colsMetadata);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "SimpleDelimited");
    }

    public void testShouldLoadRecordsIntoTable() throws Exception {
        DataLoader loader = new FixedColumnsDataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "simpledelimited";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(), tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}
