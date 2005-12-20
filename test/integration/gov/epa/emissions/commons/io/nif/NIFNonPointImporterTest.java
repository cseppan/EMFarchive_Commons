package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.nif.nonpointNonroad.NIFNonPointImporter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NIFNonPointImporterTest extends PersistenceTestCase {

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
        try {
            dataset.setInternalSources(createAllInternalSources());
            NIFNonPointImporter importer = new NIFNonPointImporter(dataset, datasource, sqlDataTypes);
            importer.run();
            assertEquals(1, countRecords(tableCE));
            assertEquals(21, countRecords(tableEM));
            assertEquals(4, countRecords(tableEP));
            assertEquals(4, countRecords(tablePE));
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            dropTables();
        }
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        dataset.setInternalSources(create_CE_EP_InternalSources());
        try {
            NIFNonPointImporter importer = new NIFNonPointImporter(dataset, datasource, sqlDataTypes);
            assertTrue(false);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF nonpoint import requires following file types"));
        }
    }

    private InternalSource[] create_CE_EP_InternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/nonpoint";
        sources.add(internalSource(new File(dir, "ky_ce.txt"), tableCE));
        sources.add(internalSource(new File(dir, "ky_ep.txt"), tableEP));

        return (InternalSource[]) sources.toArray(new InternalSource[0]);
    }

    private InternalSource[] createAllInternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/nonpoint";
        sources.add(internalSource(new File(dir, "ky_ce.txt"), tableCE));
        sources.add(internalSource(new File(dir, "ky_em.txt"), tableEM));
        sources.add(internalSource(new File(dir, "ky_ep.txt"), tableEP));
        sources.add(internalSource(new File(dir, "ky_pe.txt"), tablePE));
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
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableCE);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tablePE);
    }
}
