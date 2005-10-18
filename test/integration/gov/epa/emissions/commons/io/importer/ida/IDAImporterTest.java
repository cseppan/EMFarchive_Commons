package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.Random;

public class IDAImporterTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallAreaFile() throws Exception {
        File file = new File("test/data/ida/small-area.txt");

        IDAAreaImporter importer = new IDAAreaImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportASmallPointFile() throws Exception {
        File file = new File("test/data/ida/small-point.txt");

        IDAPointImporter importer = new IDAPointImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportASmallMobileFile() throws Exception {
        File file = new File("test/data/ida/small-mobile.txt");

        IDAMobileImporter importer = new IDAMobileImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void FIXME_testShouldImportASmallActivityFile() throws Exception {
        File file = new File("test/data/ida/small-activity.txt");

        IDAActivityImporter importer = new IDAActivityImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        TableReader tableReader = new TableReader(datasource.getConnection());
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }

}
