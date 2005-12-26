package gov.epa.emissions.commons.io.nif;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Random;

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
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonPointImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonPointTableImporter;

public class NIFNonPointTableImporterTest extends PersistenceTestCase {

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
        // first import the files
        Importer importer = new NIFNonPointImporter(files(), dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(1, countRecords(tableCE));
        assertEquals(21, countRecords(tableEM));
        assertEquals(4, countRecords(tableEP));
        assertEquals(4, countRecords(tablePE));
        String[] tables = { tableCE, tableEM, tableEP, tablePE };
        Importer tableImporter = new NIFNonPointTableImporter(tables, dataset, datasource, sqlDataTypes);
        tableImporter.run();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmm");
        assertEquals("20020101 0000", dateFormat.format(dataset.getStartDateTime()));
        assertEquals("20021231 2359", dateFormat.format(dataset.getStopDateTime()));
        assertEquals("TON", dataset.getUnits());
    }

    public void testShouldCheckForRequiredTables() throws Exception {
        try {
            Importer importer = new NIFNonPointImporter(files(), dataset, datasource, sqlDataTypes);
            importer.run();

            String[] tables = { tableCE, tableEP };
            new NIFNonPointTableImporter(tables, dataset, datasource, sqlDataTypes);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonpoint import requires following types "));
            return;
        }

        fail("Should have failed as not all tables are specified");
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

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableCE);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
