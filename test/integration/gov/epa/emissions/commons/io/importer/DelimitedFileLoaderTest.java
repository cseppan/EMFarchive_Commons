package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class DelimitedFileLoaderTest extends DbTestCase {

    private Reader reader;

    private Datasource datasource;

    private SqlDataTypes dataType;

    private TableColumnsMetadata colsMetadata;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        dataType = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/orl/SimpleDelimited.txt");
        reader = new DelimitedFileReader(file);

        colsMetadata = new TableColumnsMetadata(new DelimitedColumnsMetadata(7, dataType), dataType);
        createTable("SimpleDelimited", colsMetadata);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "SimpleDelimited");
    }

    private void createTable(String table, ColumnsMetadata colsMetadata) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(datasource.getName(), table, colsMetadata.colNames(), colsMetadata.colTypes());
    }

    public void testShouldLoadRecordsIntoTable() throws Exception {
        DataLoader loader = new DataLoader(datasource, colsMetadata);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        String tableName = "simpledelimited";

        loader.load(reader, dataset, tableName);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}
