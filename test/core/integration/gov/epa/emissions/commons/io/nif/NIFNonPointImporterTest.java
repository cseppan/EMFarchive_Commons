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
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonPointImporter;

import java.io.File;
import java.sql.SQLException;
import java.util.Random;

public class NIFNonPointImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tablePE;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        String name = dataset.getName();
        tableCE = name + "_ce";
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFiles() throws Exception {
        try {
            File folder = new File("test/data/nif/nonpoint");
            String[] files = {"ky_ce.txt", "ky_em.txt", "ky_ep.txt", "ky_pe.txt"};
            NIFNonPointImporter importer = new NIFNonPointImporter(folder, files, dataset, dbServer, sqlDataTypes);
            importer.run();
            assertEquals(1, countRecords(tableCE));
            assertEquals(21, countRecords(tableEM));
            assertEquals(4, countRecords(tableEP));
            assertEquals(4, countRecords(tablePE));
        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        try {
            File folder = new File("test/data/nif/nonpoint");
            String[] files = {"ky_ce.txt", "ky_ep.txt"};
            new NIFNonPointImporter(folder, files, dataset, dbServer, sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonpoint import requires following types"));
            return;
        }

        fail("Should have failed as the required types were not specified");
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
        dbUpdate.dropTable(datasource.getName(), tableCE);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
