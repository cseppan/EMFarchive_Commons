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
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadImporter;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadSummary;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NIFOnRoadSummaryTest extends PersistenceTestCase {

    private Datasource emissionDatasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableEM;

    private String tablePE;

    private String tableTR;

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
        tableEM = name + "_em";
        tablePE = name + "_pe";
        tableTR = name + "_tr";
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        dataset.setInternalSources(createAllInternalSources());
        try {
            NIFOnRoadImporter importer = new NIFOnRoadImporter(dataset, emissionDatasource, sqlDataTypes);
            importer.run();
            SummaryTable summary = new NIFOnRoadSummary(emissionDatasource, referenceDatasource, dataset);
            summary.createSummary();
            assertEquals(10, countRecords(tableEM));
            assertEquals(10, countRecords(tablePE));
            assertEquals(8, countRecords(tableTR));
            assertEquals(2, countRecords("test_summary"));
        } finally {
            dropTables();
        }
    }

    private InternalSource[] createAllInternalSources() {
        List sources = new ArrayList();
        String dir = "test/data/nif/onroad";
        sources.add(internalSource(new File(dir, "ct_em.txt"), tableEM));
        sources.add(internalSource(new File(dir, "ct_pe.txt"), tablePE));
        sources.add(internalSource(new File(dir, "ct_tr.txt"), tableTR));
        return (InternalSource[]) sources.toArray(new InternalSource[0]);
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
        DbUpdate dbUpdate = new DbUpdate(emissionDatasource.getConnection());
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), tableTR);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");

    }

}
