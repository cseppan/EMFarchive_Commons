package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.SummaryTable;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
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
            NIFPointImporter importer = new NIFPointImporter(files(), dataset, emissionDatasource, sqlDataTypes);
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

    private File[] files() {
        String dir = "test/data/nif/point";
        return new File[] { new File(dir, "ky_ce.txt"), new File(dir, "ky_em.txt"), new File(dir, "ky_ep.txt"),
                new File(dir, "ky_er.txt"), new File(dir, "ky_eu.txt"), new File(dir, "ky_pe.txt"),
                new File(dir, "ky_si.txt") };
    }

    private InternalSource internalSource(File file, String table) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setSource(file.getAbsolutePath());
        source.setSourceSize(file.length());

        return source;
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

}
