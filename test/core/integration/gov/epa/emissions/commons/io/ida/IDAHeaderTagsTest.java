package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.sql.SQLException;
import java.util.Random;

public class IDAHeaderTagsTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private SimpleDataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();
        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {// no op
    }

    private void dropTable() throws Exception, SQLException {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldIdentifyAllRequiredTags() throws Exception {
        File file = new File("test/data/ida/small-area.txt");
        IDAImporter importer = new IDAImporter(dataset, dbServer, sqlDataTypes);
        importer.setup(file, new IDANonPointNonRoadFileFormat(sqlDataTypes));
        importer.run();
        dropTable();
    }

    public void testShouldIdentifyNoIDATag() throws Exception {
        File file = new File("test/data/ida/noIDATags.txt");
        try {
            IDAImporter importer = new IDAImporter(dataset, dbServer, sqlDataTypes);
            importer.setup(file, new IDANonPointNonRoadFileFormat(sqlDataTypes));
            importer.run();
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("The tag - 'IDA' is mandatory"));
            return;
        }

        fail("Should have failed as IDA tag is mandatory");
    }

}
