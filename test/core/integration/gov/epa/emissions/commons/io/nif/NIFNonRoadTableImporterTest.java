package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadTableImporter;

import java.io.File;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Random;

public class NIFNonRoadTableImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

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
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointTables() throws Exception {
        try {
            File folder = new File("test/data/nif/nonroad");
            String[] files = {"ct_em.txt", "ct_ep.txt", "ct_pe.txt"};
            NIFNonRoadImporter importer = new NIFNonRoadImporter(folder, files, dataset, datasource, sqlDataTypes);
            importer.run();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tableEP));
            assertEquals(10, countRecords(tablePE));
            String[] tables = { tableEM, tableEP, tablePE };
            Importer tableImporter = new NIFNonRoadTableImporter(tables, dataset, datasource, sqlDataTypes);
            tableImporter.run();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmm");
            assertEquals("19990101 0000", dateFormat.format(dataset.getStartDateTime()));
            assertEquals("19991231 2359", dateFormat.format(dataset.getStopDateTime()));
            assertEquals("TON", dataset.getUnits());

        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredTables() throws Exception {
        File folder = new File("test/data/nif/nonroad");
        String[] files = {"ct_em.txt", "ct_ep.txt", "ct_pe.txt"};
        NIFNonRoadImporter importer = new NIFNonRoadImporter(folder, files, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(10, countRecords(tableEM));
        assertEquals(10, countRecords(tableEP));
        assertEquals(10, countRecords(tablePE));
        String[] tables = { tableEP, tablePE };
        try {
            new NIFNonRoadTableImporter(tables, dataset, datasource, sqlDataTypes);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("NIF nonroad import requires following types "));
            return;
        } finally {
            dropTables();
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
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
