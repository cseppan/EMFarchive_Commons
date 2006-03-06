package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.HibernateTestCase;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.Versions;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class LineImporterTest extends HibernateTestCase {


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

    public void testShouldImportASmallLineFile() throws Exception {
        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[] { "small-point.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        assertEquals(22, countRecords());
    }

    public void testShouldImportASmallVersionedLineFile() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[] { "small-point.txt" }, dataset, dbServer,
                sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();

        int rows = countRecords();
        assertEquals(22, rows);
        assertVersionInfo(dataset.getName(), rows);
    }

    private int countRecords() {
        Datasource datasource =dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }

    private void assertVersionInfo(String name, int rows) throws Exception {
        verifyVersionCols(name, rows);
        verifyVersionZeroEntryInVersionsTable();
    }

    private void verifyVersionCols(String table, int rows) throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);

        ITable tableRef = tableReader.table(datasource.getName(), table);
        for (int i = 0; i < rows; i++) {
            Object recordId = tableRef.getValue(i, "Record_Id");
            assertEquals((i + 1) + "", recordId.toString());

            Object version = tableRef.getValue(i, "Version");
            assertEquals("0", version.toString());

            Object deleteVersions = tableRef.getValue(i, "Delete_Versions");
            assertEquals("", deleteVersions);
        }
    }

    private void verifyVersionZeroEntryInVersionsTable() throws Exception {
        Versions versions = new Versions();
        Version[] simpleVersions = versions.get(dataset.getId(), session);
        assertEquals(1, simpleVersions.length);

        Version versionZero = simpleVersions[0];
        assertEquals(0, versionZero.getVersion());
        assertEquals(dataset.getId(), versionZero.getDatasetId());
        assertEquals("", versionZero.getPath());
        assertTrue("Version Zero should be zero upon import", versionZero.isFinalVersion());
    }

}
