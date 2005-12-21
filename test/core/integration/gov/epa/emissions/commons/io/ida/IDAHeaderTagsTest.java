package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class IDAHeaderTagsTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Datasource emissionDatasource;

    private SimpleDataset dataset;

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

    private void dropTable() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(emissionDatasource);
        dbUpdate.dropTable(emissionDatasource.getName(), dataset.getName());
    }

    public void testShouldIdentifyAllRequiredTags() throws Exception {
        File file = new File("test/data/ida/small-area.txt");
        IDAImporter importer = new IDAImporter(dataset, emissionDatasource, referenceDatasource, sqlDataTypes);
        importer.setup(file, new IDANonPointNonRoadFileFormat(sqlDataTypes));
        importer.run();
        dropTable();
    }



    public void itestShouldIdentifyNoIDATag() throws Exception {
        File file = new File("test/data/ida/noIDATags.txt");
        try {
            IDAImporter importer = new IDAImporter(dataset, emissionDatasource, referenceDatasource, sqlDataTypes);
            importer.setup(file, new IDANonPointNonRoadFileFormat(sqlDataTypes));
            importer.run();
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("The tag - 'IDA' is mandatory"));
        }
    }

}
