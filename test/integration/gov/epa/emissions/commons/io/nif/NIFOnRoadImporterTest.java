package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.onroad.NIFOnRoadImporter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NIFOnRoadImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableEM;

    private String tablePE;

    private String tableTR;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());

        String name = dataset.getName();
        tableEM = name + "_em";
        tablePE = name + "_pe";
        tableTR = name + "_tr";

    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        dataset.setInternalSources(createAllInternalSources());

        NIFOnRoadImporter importer = new NIFOnRoadImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null,null);
        importer.run(dataset);
        assertEquals(10, countRecords(tableEM));
        assertEquals(10, countRecords(tablePE));
        assertEquals(8, countRecords(tableTR));
        dropTables();
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        dataset.setInternalSources(create_EP_InternalSources());

        NIFOnRoadImporter importer = new NIFOnRoadImporter(dataset, datasource, sqlDataTypes);
        try {
            importer.preCondition(null,null);
            assertTrue(false);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF onroad import requires following file types"));
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

    private InternalSource[] create_EP_InternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/onroad";
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
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tablePE);
        dbUpdate.dropTable(datasource.getName(), tableTR);
    }

}
