package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.point.NIFPointImporter;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class NIFPointImporterTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private String tableCE;

    private String tableEM;

    private String tableEP;

    private String tableER;

    private String tableEU;

    private String tablePE;

    private String tableSI;

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
        tableER = name + "_er";
        tableEU = name + "_eu";
        tablePE = name + "_pe";
        tableSI = name + "_si";
    }

    public void testShouldImportASmallAndSimplePointFiles() throws Exception {
        dataset.setInternalSources(createAllInternalSources());
        
        NIFPointImporter importer = new NIFPointImporter(dataset, datasource, sqlDataTypes);
        importer.preCondition(null,null);
        importer.run(dataset);
        assertEquals(92, countRecords(tableCE));
        assertEquals(143, countRecords(tableEM));
        assertEquals(26, countRecords(tableEP));
        assertEquals(15, countRecords(tableER));
        assertEquals(15, countRecords(tableEU));
        assertEquals(26, countRecords(tablePE));
        assertEquals(1, countRecords(tableSI));
        dropTables();
    }

    public void testShouldCheckForReuiredInternalSources() throws Exception {
        dataset.setInternalSources(create_CE_EP_InternalSources());
        
        NIFPointImporter importer = new NIFPointImporter(dataset, datasource, sqlDataTypes);
        try {
            importer.preCondition(null,null);
            assertTrue(false);
        } catch (ImporterException e) {
            assertTrue(e.getMessage().startsWith("NIF point import requires following file types"));
        }
    }

    private InternalSource[] createAllInternalSources() {
        List sources = new ArrayList();
        String dir = "test/data/nif/point";
        sources.add(internalSource(new File(dir, "ky_ce.txt"), tableCE));
        sources.add(internalSource(new File(dir, "ky_em.txt"), tableEM));
        sources.add(internalSource(new File(dir, "ky_ep.txt"), tableEP));
        sources.add(internalSource(new File(dir, "ky_er.txt"), tableER));
        sources.add(internalSource(new File(dir, "ky_eu.txt"), tableEU));
        sources.add(internalSource(new File(dir, "ky_pe.txt"), tablePE));
        sources.add(internalSource(new File(dir, "ky_si.txt"), tableSI));
        return (InternalSource[]) sources.toArray(new InternalSource[0]);
    }

    private InternalSource internalSource(File file, String table) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setSource(file.getAbsolutePath());
        source.setSourceSize(file.length());

        return source;
    }

    private InternalSource[] create_CE_EP_InternalSources() {
        List sources = new ArrayList();

        String dir = "test/data/nif/point";
        sources.add(internalSource(new File(dir, "ky_ce.txt"), tableCE));
        sources.add(internalSource(new File(dir, "ky_ep.txt"), tableEP));

        return (InternalSource[]) sources.toArray(new InternalSource[0]);
    }

    private int countRecords(String tableName) {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), tableName);
    }

    protected void dropTables() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), tableCE);
        dbUpdate.dropTable(datasource.getName(), tableEM);
        dbUpdate.dropTable(datasource.getName(), tableEP);
        dbUpdate.dropTable(datasource.getName(), tableER);
        dbUpdate.dropTable(datasource.getName(), tableEU);
        dbUpdate.dropTable(datasource.getName(), tablePE);
        dbUpdate.dropTable(datasource.getName(), tableSI);
    }

}
