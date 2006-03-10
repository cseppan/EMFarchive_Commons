package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.SummaryTable;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonpointNonRoadSummary;

import java.io.File;
import java.util.Random;

public class NIFNonRoadSummaryTest extends PersistenceTestCase {

    private Datasource emissionDatasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

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
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFiles() throws Exception {
        File folder = new File("test/data/nif/nonroad");
        String[] files = {"ct_em.txt", "ct_ep.txt", "ct_pe.txt"};
        NIFNonRoadImporter importer = new NIFNonRoadImporter(folder, files, dataset, dbServer(), sqlDataTypes);
        importer.run();
        SummaryTable summary = new NIFNonpointNonRoadSummary(emissionDatasource, referenceDatasource, dataset);
        summary.createSummary();
        assertEquals(10, countRecords(tableEM));
        assertEquals(10, countRecords(tableEP));
        assertEquals(10, countRecords(tablePE));
        assertEquals(0, countRecords("test_summary"));
    }

    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(emissionDatasource);
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(emissionDatasource);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEP);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }
}
