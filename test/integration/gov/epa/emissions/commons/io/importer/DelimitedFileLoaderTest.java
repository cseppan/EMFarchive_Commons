package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataType;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.sql.SQLException;

public class DelimitedFileLoaderTest extends DbTestCase {

    private DelimitedFileReader reader;

    private Datasource datasource;

    private SqlDataType typeMapper;


    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getTypeMapper();
        datasource = dbServer.getEmissionsDatasource();

        File file = new File("test/data/orl/SimpleDelimited.txt");
        reader = new DelimitedFileReader(file);

        createTable("SimpleDelimited");
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), "SimpleDelimited");
    }

    private void createTable(String table) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        String [] colNames = {"FIPS","PLANTID","SCC","NAIC","MACT","SRCTYPE","POINTID"};
        String varcharType = typeMapper.getString(20);
        String [] colTypes = {varcharType,varcharType,varcharType,varcharType,varcharType,varcharType, varcharType};
        tableDefinition.createTable(datasource.getName(), table, colNames, colTypes);
    }

    public void testShouldLoadRecordsIntoTable() throws Exception {
        DelimitedFileLoader loader = new DelimitedFileLoader(datasource);

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        loader.load(dataset, reader);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        String tableName = "simpledelimited";

        assertTrue("Table '" + tableName + "' should have been created", tableReader.exists(datasource.getName(),
                tableName));
        assertEquals(10, tableReader.count(datasource.getName(), tableName));
    }
}
