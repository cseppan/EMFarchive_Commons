package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

public class InventoryTableImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testImportInventoryTableData() throws Exception {
        File folder = new File("test/data/other");
        InventoryTableImporter importer = new InventoryTableImporter(folder,
                new String[] { "CAPandHAP_INVTABLE31aug2006.txt" }, dataset, dbServer, sqlDataTypes);
        importer.run();

        assertEquals(598, countRecords());
    }

    public void testImportVersionedInventoryTableData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        InventoryTableImporter importer = new InventoryTableImporter(folder,
                new String[] { "CAPandHAP_INVTABLE31aug2006.txt" }, dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        assertEquals(598, countRecords());
    }

    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
