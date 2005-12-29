package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.Versions;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class LineImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;
    
    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallLineFile() throws Exception {
        File file = new File("test/data/orl/nc","small-point.txt");
        LineImporter importer = new LineImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        assertEquals(22, countRecords());
    }
    
    public void testShouldImportASmallVersionedLineFile() throws Exception {
        File file = new File("test/data/orl/nc","small-point.txt");
        LineImporter lineImporter = new LineImporter(file, dataset, datasource, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        VersionedImporter importer = new VersionedImporter(lineImporter, dataset, datasource);
        importer.run();
        
        int rows = countRecords();
        assertEquals(22, rows);
        assertVersionInfo(dataset.getName(), rows);
    }
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }

    private void assertVersionInfo(String name, int rows) throws Exception {
        verifyVersionCols(name, rows);
        verifyVersionZeroEntryInVersionsTable();
    }
    
    private void verifyVersionCols(String table, int rows) throws Exception {
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
        Versions versions = new Versions(datasource);
        Version[] simpleVersions = versions.get(dataset.getDatasetid());
        assertEquals(1, simpleVersions.length);

        Version versionZero = simpleVersions[0];
        assertEquals(0, versionZero.getVersion());
        assertEquals(dataset.getDatasetid(), versionZero.getDatasetId());
        assertEquals("", versionZero.getPath());
        assertTrue("Version Zero should be zero upon import", versionZero.isFinalVersion());
    }

}
