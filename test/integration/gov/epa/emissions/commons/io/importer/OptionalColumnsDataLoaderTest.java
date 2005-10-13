package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;

public class OptionalColumnsDataLoaderTest extends DbTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes dataTypes;

    private OptionalColumnsTableMetadata colsMetadata;

    private String table;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        dataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/variable-cols.txt");
        reader = new DelimitedFileReader(file);

        colsMetadata = new OptionalColumnsTableMetadata(new PointSourceTemporalCrossReferenceColumnsMetadata(dataTypes), dataTypes);
        table = "varying";
        createTable(table, datasource, colsMetadata);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), table);
    }

    public void testShouldLoadRecordsIntoTable() throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        loader.load(reader, dataset, table);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + table + "' should have been created", tableReader.exists(datasource.getName(), table));
        assertEquals(20, tableReader.count(datasource.getName(), table));
    }
}
