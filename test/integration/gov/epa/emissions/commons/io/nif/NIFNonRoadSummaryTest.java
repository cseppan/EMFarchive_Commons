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
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonpointNonRoadSummary;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
        dataset.setInternalSources(createEM_EP_PE_InternalSources());
        try {
            NIFNonRoadImporter importer = new NIFNonRoadImporter(dataset, emissionDatasource, sqlDataTypes);
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

    private InternalSource[] createEM_EP_PE_InternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/nonroad";
        sources.add(internalSource(new File(dir, "ct_em.txt"), tableEM));
        sources.add(internalSource(new File(dir, "ct_ep.txt"), tableEP));
        sources.add(internalSource(new File(dir, "ct_pe.txt"), tablePE));
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
        TableReader tableReader = new TableReader(emissionDatasource.getConnection());
        return tableReader.count(emissionDatasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(emissionDatasource.getConnection());
        dbUpdate.dropTable(emissionDatasource.getName(), tableEM);
        dbUpdate.dropTable(emissionDatasource.getName(), tableEP);
        dbUpdate.dropTable(emissionDatasource.getName(), tablePE);
        dbUpdate.dropTable(emissionDatasource.getName(), "test_summary");
    }
}

