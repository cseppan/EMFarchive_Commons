package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.SummaryTable;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonpointNonRoadSummary;

import java.io.File;
import java.util.Random;

public class NIFNonRoadSummaryTest extends PersistenceTestCase {

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
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        String name = dataset.getName();
        tableCE = name + "_ce";
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFiles() throws Exception {
        try {
            NIFNonRoadImporter importer = new NIFNonRoadImporter(files(), dataset, emissionDatasource, sqlDataTypes);
            importer.run();
            SummaryTable summary = new NIFNonpointNonRoadSummary(emissionDatasource,referenceDatasource,dataset);
            summary.createSummary();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tableEP));
            assertEquals(10, countRecords(tablePE));
            assertEquals(0,countRecords("test_summary"));
        } finally {
            dropTables();
        }
    }

    private File[] files() {
        String dir = "test/data/nif/nonroad";
        return new File[]{ new File(dir, "ct_em.txt"), new File(dir, "ct_ep.txt"), new File(dir, "ct_pe.txt")};
    }


    private int countRecords(String tableName) {
        TableReader tableReader = tableReader(emissionDatasource);
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(emissionDatasource);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEP);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }
}

