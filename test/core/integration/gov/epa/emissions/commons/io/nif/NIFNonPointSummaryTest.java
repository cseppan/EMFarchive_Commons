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
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonPointImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonpointNonRoadSummary;

import java.io.File;
import java.util.Random;

public class NIFNonPointSummaryTest extends PersistenceTestCase {

    private Datasource emissionDatasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tablePE;

    private Datasource referenceDatasource;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        emissionDatasource = dbServer.getEmissionsDatasource();
        referenceDatasource = dbServer.getReferenceDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));

        String name = dataset.getName();
        tableCE = name + "_ce";
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFilesAndCreateSummary() throws Exception {
        File folder = new File("test/data/nif/nonpoint");
        String[] files = {"ky_ce.txt", "ky_em.txt", "ky_ep.txt", "ky_pe.txt"};
        NIFNonPointImporter importer = new NIFNonPointImporter(folder, files, 
                dataset, dbServer(), sqlDataTypes);
        SummaryTable summary = new NIFNonpointNonRoadSummary(emissionDatasource, referenceDatasource, dataset);
        importer.run();
        summary.createSummary();

        assertEquals(1, countRecords(tableCE));
        assertEquals(21, countRecords(tableEM));
        assertEquals(4, countRecords(tableEP));
        assertEquals(4, countRecords(tablePE));
        assertEquals(3, countRecords("test_summary"));
    }

    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(emissionDatasource);
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(emissionDatasource);
        dbUpdate.dropTable(emissionDatasource.getName(), tableCE);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEP);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }
}
