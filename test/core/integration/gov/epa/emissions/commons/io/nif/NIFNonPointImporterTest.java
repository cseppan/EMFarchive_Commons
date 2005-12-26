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

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tablePE;

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
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFiles() throws Exception {
        try {
            NIFNonPointImporter importer = new NIFNonPointImporter(files(), dataset, datasource, sqlDataTypes);
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
            new NIFNonPointImporter(files_CE_EP(), dataset, datasource, sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonpoint import requires following types"));
            return;
        }

        fail("Should have failed as the required types were not specified");
    }

    private File[] files_CE_EP() {
        String dir = "test/data/nif/nonpoint";
        return new File[] { new File(dir, "ky_ce.txt"), new File(dir, "ky_ep.txt") };
    }

    private File[] files() {
        String dir = "test/data/nif/nonpoint";
        return new File[] { new File(dir, "ky_ce.txt"), new File(dir, "ky_em.txt"), new File(dir, "ky_ep.txt"),
                new File(dir, "ky_pe.txt") };
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
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
