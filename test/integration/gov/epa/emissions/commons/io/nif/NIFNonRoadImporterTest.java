package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonRoadImporter;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NIFNonRoadImporterTest extends DbTestCase {

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
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());

        String name = dataset.getName();
        tableCE = name + "_ce";
        tableEM = name + "_em";
        tableEP = name + "_ep";
        tablePE = name + "_pe";
    }

    public void testShouldImportAAllNonPointFiles() throws Exception {
        dataset.setInternalSources(createEM_EP_PE_InternalSources());
        NIFNonRoadImporter importer = new NIFNonRoadImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null,null);
        importer.run(dataset);
        assertEquals(10, countRecords(tableEM));
        assertEquals(10, countRecords(tableEP));
        assertEquals(10, countRecords(tablePE));
        dropTables();
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        dataset.setInternalSources(create_EP_PE_InternalSources());
        NIFNonRoadImporter importer = new NIFNonRoadImporter(dataset, datasource, sqlDataTypes);
        try {
            importer.preCondition(null,null);
            assertTrue(false);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonroad import requires following file types"));
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

    private InternalSource[] create_EP_PE_InternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/nonroad";
        sources.add(internalSource(new File(dir, "ct_ep.txt"), tableEP));
        sources.add(internalSource(new File(dir, "ct_pe.txt"), tableCE));

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
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
