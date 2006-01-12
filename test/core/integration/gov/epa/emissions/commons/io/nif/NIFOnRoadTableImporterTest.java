package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadImporter;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadTableImporter;

import java.io.File;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Random;

public class NIFOnRoadTableImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableEM;

    private String tablePE;

    private String tableTR;

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
        tablePE = name + "_pe";
        tableTR = name + "_tr";
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        try {
            File folder = new File("test/data/nif/onroad");
            String[] files = {"ct_em.txt", "ct_pe.txt", "ct_tr.txt"};
            NIFOnRoadImporter importer = new NIFOnRoadImporter(folder, files, dataset, dbServer(), sqlDataTypes);
            importer.run();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tablePE));
            assertEquals(8, countRecords(tableTR));
            String tables[] = {tableEM,tablePE,tableTR};
            Importer tableImporter = new NIFOnRoadTableImporter(tables,dataset,dbServer(),sqlDataTypes);
            tableImporter.run();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmm");
            assertEquals("19990101 0000", dateFormat.format(dataset.getStartDateTime()));
            assertEquals("19991231 2359", dateFormat.format(dataset.getStopDateTime()));
            assertEquals("TON", dataset.getUnits());
        } finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        File folder = new File("test/data/nif/onroad");
        String[] files = {"ct_em.txt", "ct_pe.txt", "ct_tr.txt"};
        NIFOnRoadImporter importer = new NIFOnRoadImporter(folder, files, dataset, dbServer(), sqlDataTypes);
        importer.run();
        assertEquals(10, countRecords(tableEM));
        assertEquals(10, countRecords(tablePE));
        assertEquals(8, countRecords(tableTR));
        String tables[] = {tablePE};
        
        try {
            new NIFOnRoadTableImporter(tables,dataset,dbServer(),sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF onroad import requires following types"));
            return;
        }finally{
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
        dbUpdate.dropTable(datasource.getName(), tablePE);
        dbUpdate.dropTable(datasource.getName(), tableTR);
    }

}
