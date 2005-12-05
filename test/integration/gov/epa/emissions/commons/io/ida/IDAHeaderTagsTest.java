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

    private Datasource datasource;

    private SimpleDataset dataset;

    protected void setUp() throws Exception {
        super.setUp();
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    private void dropTable() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldIdentifyAllRequiredTags() throws Exception {
        File file = new File("test/data/ida/small-area.txt");
        IDAImporter importer = new IDAImporter(dataset, datasource, sqlDataTypes);
        importer.setup(file, new IDANonPointFileFormat(sqlDataTypes));
        importer.run();
        dropTable();
    }



    public void testShouldIdentifyNoIDATag() throws Exception {
        File file = new File("test/data/ida/noIDATags.txt");
        try {
            IDAImporter importer = new IDAImporter(dataset, datasource, sqlDataTypes);
            importer.setup(file, new IDANonPointFileFormat(sqlDataTypes));
            importer.run();
            assertTrue(false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            assertTrue(e.getMessage().startsWith("The tag - 'IDA' is mandatory"));
        }
    }

}
