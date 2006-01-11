package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadImporter;

import java.io.File;
import java.sql.SQLException;
import java.util.Random;

public class NIFOnRoadImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableEM;

    private String tablePE;

    private String tableTR;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        String name = dataset.getName();
        tableEM = name + "_em";
        tablePE = name + "_pe";
        tableTR = name + "_tr";
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        try {
            File folder = new File("test/data/nif/onroad");
            String[] files = {"ct_em.txt", "ct_pe.txt", "ct_tr.txt"};
            NIFOnRoadImporter importer = new NIFOnRoadImporter(folder, files, dataset, dbServer, sqlDataTypes);
            importer.run();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tablePE));
            assertEquals(8, countRecords(tableTR));
        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        try {
            File folder = new File("test/data/nif/onroad");
            String[] files = {"ct_pe.txt"};
            new NIFOnRoadImporter(folder, files, dataset, dbServer, sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF onroad import requires following types"));
            return;
        }

        fail("Should have failed as required types are unspecified");
    }

    private int countRecords(String tableName) {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void doTearDown() throws Exception {// no op

    }

    private void dropTables() throws Exception, SQLException {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tablePE);
        dbUpdate.dropTable(datasource.getName(), tableTR);
    }

}
