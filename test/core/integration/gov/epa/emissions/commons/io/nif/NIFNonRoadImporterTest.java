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
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;

import java.io.File;
import java.util.Random;

public class NIFNonRoadImporterTest extends PersistenceTestCase {

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
            NIFNonRoadImporter importer = new NIFNonRoadImporter(files(), dataset, datasource, sqlDataTypes);
            importer.run();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tableEP));
            assertEquals(10, countRecords(tablePE));
        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        try {
            new NIFNonRoadImporter(files_EP_PE(), dataset, datasource, sqlDataTypes);
            assertTrue(false);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonroad import requires following file types"));
        }
    }

    private File[] files() {
        String dir = "test/data/nif/nonroad";
        return new File[] { new File(dir, "ct_em.txt"), new File(dir, "ct_ep.txt"), new File(dir, "ct_pe.txt") };
    }

    private File[] files_EP_PE() {
        String dir = "test/data/nif/nonroad";
        return new File[] { new File(dir, "ct_ep.txt"), new File(dir, "ct_pe.txt") };
    }

    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
