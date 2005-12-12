package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.Versions;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.orl.ORLOnRoadExporter;
import gov.epa.emissions.commons.io.orl.ORLOnRoadImporter;

import java.io.File;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class TemporalReferenceVersionImporterTest extends PersistenceTestCase {
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
        
        /*HelpImporter delegate = new HelpImporter();
        FileFormat fileFormat = new TemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new VersionedTemporalTableFormat(fileFormat, sqlDataTypes);
        String table = delegate.tableName(dataset.getName());
        FormatUnit formatUnit = new DatasetTypeUnit(tableFormat, fileFormat);
        delegate.createTable(table, datasource, formatUnit.tableFormat(), dataset.getName());
        */
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());

        dbUpdate.deleteAll(datasource.getName(), "versions");
    }

    public void testShouldImportReferenceFile() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        TemporalReferenceImporter importer = new TemporalReferenceImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(34, rows);
        assertVersionInfo(dataset.getName(), rows);
        
        TemporalReferenceExporter exporter = new TemporalReferenceExporter(dataset, 
                datasource, sqlDataTypes);
        File exportfile = new File("test/data/temporal-crossreference","VersionedCrossRefExported.txt");
        exporter.export(exportfile);
        //FIXME: compare the original file and the exported file.
        exportfile.delete();
    }
        
    public void ShouldExportReferenceVersionZero() throws Exception {
        File importFile = new File("test/data/orl/nc","small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(importFile, dataset, datasource, sqlDataTypes);
        importer.run();
        File exportfile = new File("test/data/temporal-crossreference","VersionedCrossRefExported.txt");
        Exporter exporter = new ORLOnRoadExporter(dataset, datasource, sqlDataTypes);
        exporter.export(0, exportfile);
    }
    

    private void assertVersionInfo(String name, int rows) throws Exception {
        verifyVersionCols(name, rows);
        verifyVersionZeroEntryInVersionsTable();
    }

    private void verifyVersionCols(String table, int rows) throws Exception {
        TableReader tableReader = new TableReader(datasource.getConnection());

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
        Version[] onRoadVersions = versions.get(dataset.getDatasetid());
        assertEquals(1, onRoadVersions.length);

        Version versionZero = onRoadVersions[0];
        assertEquals(0, versionZero.getVersion());
        assertEquals(dataset.getDatasetid(), versionZero.getDatasetId());
        assertEquals("", versionZero.getPath());
        assertTrue("Version Zero should be zero upon import", versionZero.isFinalVersion());
    }

    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
