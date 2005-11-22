package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.temporal.PointTemporalReferenceFileFormat;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;

public class OptionalColumnsDataLoaderTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes dataTypes;

    private TableFormatWithOptionalCols colsMetadata;

    private String table;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        dataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        colsMetadata = new TableFormatWithOptionalCols(new PointTemporalReferenceFileFormat(dataTypes), dataTypes);
        table = "varying";
        createTable(table, datasource, colsMetadata);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), table);
    }

    public void testShouldLoadRecordsFromFileWithVariableColsIntoTable() throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        File file = new File("test/data/variable-cols.txt");
        Reader reader = new WhitespaceDelimitedFileReader(file);
        loader.load(reader, dataset, table);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + table + "' should have been created", tableReader.exists(datasource.getName(), table));
        assertEquals(6, tableReader.count(datasource.getName(), table));
    }

    public void testShouldFailToLoadRecordsAsOneOfTheRecordsHasLessThanMinCols() throws Exception {
        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        File file = new File("test/data/variable-cols-with-errors.txt");
        Reader reader = new WhitespaceDelimitedFileReader(file);
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, colsMetadata);

        try {
            loader.load(reader, dataset, table);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertEquals(0, tableReader.count(datasource.getName(), table));
            
            return;
        }

        fail("should have failed due to error in record 5 having less than min cols");
    }
}
