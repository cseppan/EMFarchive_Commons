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
import gov.epa.emissions.commons.io.nif.point.NIFPointImporter;

import java.io.File;
import java.sql.SQLException;
import java.util.Random;

public class NIFPointImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tableER;

    private String tableEU;

    private String tablePE;

    private String tableSI;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        String name = dataset.getName();
        tableCE = name + "_ce";
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tableER = name + "_er";
        tableEU = name + "_eu";
        tablePE = name + "_pe";
        tableSI = name + "_si";
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        try {
            File folder = new File("test/data/nif/point");
            String[] files = {"ky_ce.txt", "ky_em.txt", "ky_ep.txt", 
                    "ky_er.txt", "ky_eu.txt", "ky_pe.txt", "ky_si.txt"};
            NIFPointImporter importer = new NIFPointImporter(folder, files, dataset, datasource, sqlDataTypes);
            importer.run();
            assertEquals(92, countRecords(tableCE));
            assertEquals(143, countRecords(tableEM));
            assertEquals(26, countRecords(tableEP));
            assertEquals(15, countRecords(tableER));
            assertEquals(15, countRecords(tableEU));
            assertEquals(26, countRecords(tablePE));
            assertEquals(1, countRecords(tableSI));
        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        try {
            File folder = new File("test/data/nif/point");
            String[] files = {"ky_ce.txt", "ky_ep.txt"};
            new NIFPointImporter(folder, files, dataset, datasource, sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF point import requires following types"));
            return;
        }

        fail("Should have failed as required types are unspecified");
    }

    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void doTearDown() throws Exception {// no op
    }

    private void dropTables() throws Exception, SQLException {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableCE);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tableER);
        dbUpdate.dropTable(datasource.getName(), tableEU);
        dbUpdate.dropTable(datasource.getName(), tablePE);
        dbUpdate.dropTable(datasource.getName(), tableSI);
    }

}
