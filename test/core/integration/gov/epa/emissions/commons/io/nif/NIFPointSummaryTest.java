package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.SummaryTable;
import gov.epa.emissions.commons.io.nif.point.NIFPointImporter;
import gov.epa.emissions.commons.io.nif.point.NIFPointSummary;

import java.io.File;
import java.util.Random;

public class NIFPointSummaryTest extends PersistenceTestCase {

    private Datasource emissionDatasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tableER;

    private String tableEU;

    private String tablePE;

    private String tableSI;

    private Datasource referenceDatasource;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        emissionDatasource = dbServer.getEmissionsDatasource();
        referenceDatasource = dbServer.getReferenceDatasource();
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
            NIFPointImporter importer = new NIFPointImporter(folder, files, dataset, emissionDatasource, sqlDataTypes);
            importer.run();
            SummaryTable summary = new NIFPointSummary(emissionDatasource, referenceDatasource, dataset);
            summary.createSummary();
            assertEquals(92, countRecords(tableCE));
            assertEquals(143, countRecords(tableEM));
            assertEquals(26, countRecords(tableEP));
            assertEquals(15, countRecords(tableER));
            assertEquals(15, countRecords(tableEU));
            assertEquals(26, countRecords(tablePE));
            assertEquals(1, countRecords(tableSI));
            assertEquals(26, countRecords("test_summary"));
        } finally {
            dropTables();
        }
    }

    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(emissionDatasource);
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(emissionDatasource);
        dbUpdate.dropTable(emissionDatasource.getName(), tableCE);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEP);
        dbUpdate.dropTable(emissionDatasource.getName(), tableER);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEU);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), tableSI);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }

    protected void doTearDown() throws Exception {// no op
    }

}
