package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class IDAImporterTest extends PersistenceTestCase {

    private Datasource emissionDatasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private Datasource referenceDatasource;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        emissionDatasource = dbServer.getEmissionsDatasource();
        referenceDatasource = dbServer.getReferenceDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(emissionDatasource.getConnection());
        dbUpdate.dropTable(emissionDatasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallAreaFile() throws Exception {
        File file = new File("test/data/ida", "small-area.txt");
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(file, dataset, emissionDatasource,
                referenceDatasource, sqlDataTypes);
        importer.run();
        // assert
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        assertEquals(10, tableReader.count(emissionDatasource.getName(), dataset.getName()));
    }

    public void testShouldImportASmallPointFile() throws Exception {
        File file = new File("test/data/ida", "small-point.txt");
        IDAPointImporter importer = new IDAPointImporter(file, dataset, emissionDatasource, referenceDatasource,
                sqlDataTypes);
        importer.run();

        // assert
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        assertEquals(10, tableReader.count(emissionDatasource.getName(), dataset.getName()));
    }

    public void testShouldImportASmallMobileFile() throws Exception {
        File file = new File("test/data/ida", "small-mobile.txt");
        IDAMobileImporter importer = new IDAMobileImporter(file, dataset, emissionDatasource, referenceDatasource,
                sqlDataTypes);
        importer.run();
        // assert
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        assertEquals(10, tableReader.count(emissionDatasource.getName(), dataset.getName()));
    }

    public void FIXME_testShouldImportASmallActivityFile() throws Exception {
        File file = new File("test/data/ida/small-activity.txt");

        IDAActivityImporter importer = new IDAActivityImporter(file, dataset, emissionDatasource, referenceDatasource,
                sqlDataTypes);
        importer.run();

        // assert
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        assertEquals(10, tableReader.count(emissionDatasource.getName(), dataset.getName()));
    }

}
