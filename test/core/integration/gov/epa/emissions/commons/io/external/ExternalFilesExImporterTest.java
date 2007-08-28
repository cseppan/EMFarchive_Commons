package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Date;
import java.util.Random;

public class ExternalFilesExImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    private Integer optimizedBatchSize;
    
    
    protected void setUp() throws Exception {
        super.setUp();
        
        optimizedBatchSize = new Integer(10000);

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
        dataset.setDatasetType(new DatasetType("externalDSType"));
    }

    protected void doTearDown() throws Exception {
        //Nothing to tear down
    }

    public void testExportExternalFilesShouldSucceed() throws Exception {
        File folder = new File("test/data/other");
        ExternalFilesImporter importer = new ExternalFilesImporter(folder, new String[]{"pstk.m3.txt", "costcy.txt", "costcy2.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();

        ExternalFilesExporter exporter = new ExternalFilesExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        File exportfile = File.createTempFile("ExternalExported", ".txt");
        exporter.export(exportfile);
        assertEquals(3, exporter.getExportedLinesCount());
        assertEquals(3, dataset.getExternalSources().length);
        assertTrue(dataset.getExternalSources()[0].getDatasource().endsWith("pstk.m3.txt"));
        assertTrue(dataset.getExternalSources()[1].getDatasource().endsWith("costcy.txt"));
        assertTrue(dataset.getExternalSources()[2].getDatasource().endsWith("costcy2.txt"));
    }

    public void testExportVersionedExportExternalFilesShouldSucceed() throws Exception {
        DbServer localDbServer = dbSetup.getNewPostgresDbServerInstance();
        Version version = new Version();
        version.setVersion(0);
        
        File folder = new File("test/data/other");
        ExternalFilesImporter importer = new ExternalFilesImporter(folder, new String[]{"pstk.m3.txt", "costcy.txt", "costcy2.txt"},
                dataset, localDbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, localDbServer, new Date());
        importerv.run();

        ExternalFilesExporter exporter = new ExternalFilesExporter(dataset, localDbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset), optimizedBatchSize);
        File exportfile = File.createTempFile("ExternalExported", ".txt");
        exporter.export(exportfile);
        assertEquals(3, exporter.getExportedLinesCount());
        assertEquals(3, dataset.getExternalSources().length);
        assertTrue(dataset.getExternalSources()[0].getDatasource().endsWith("pstk.m3.txt"));
        assertTrue(dataset.getExternalSources()[1].getDatasource().endsWith("costcy.txt"));
        assertTrue(dataset.getExternalSources()[2].getDatasource().endsWith("costcy2.txt"));
    }
    
    public void testExportExternalFilesShouldFail() throws Exception {
        File folder = new File("test/data/other");
        ExternalFilesImporter importer = new ExternalFilesImporter(folder, new String[]{"pstk.m3.txt", "costcy.txt", "costcy2.txt", "does not exist"},
                dataset, dbServer, sqlDataTypes);
        importer.run();
        
        assertEquals(4, dataset.getExternalSources().length);
        assertTrue(dataset.getExternalSources()[0].getDatasource().endsWith("pstk.m3.txt"));
        assertTrue(dataset.getExternalSources()[1].getDatasource().endsWith("costcy.txt"));
        assertTrue(dataset.getExternalSources()[2].getDatasource().endsWith("costcy2.txt"));
        assertTrue(dataset.getExternalSources()[3].getDatasource().endsWith("does not exist"));
        
        ExternalFilesExporter exporter = new ExternalFilesExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        File exportfile = File.createTempFile("ExternalExported", ".txt");
        
        try {
            exporter.export(exportfile);
        } catch (Exception e) {
            assertEquals(3, exporter.getExportedLinesCount());
            assertTrue(e.getMessage().contains("The file " + folder.getAbsolutePath() + File.separator + "does not exist doesn't exist."));
        }
    }
    
    public void testExportVersionedExportExternalFilesShouldFail() throws Exception {
        DbServer localDbServer = dbSetup.getNewPostgresDbServerInstance();
        Version version = new Version();
        version.setVersion(0);
        
        File folder = new File("test/data/other");
        ExternalFilesImporter importer = new ExternalFilesImporter(folder, new String[]{"pstk.m3.txt", "costcy.txt", "costcy2.txt", "does not exist"},
                dataset, localDbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, localDbServer, new Date());
        importerv.run();
        
        assertEquals(4, dataset.getExternalSources().length);
        assertTrue(dataset.getExternalSources()[0].getDatasource().endsWith("pstk.m3.txt"));
        assertTrue(dataset.getExternalSources()[1].getDatasource().endsWith("costcy.txt"));
        assertTrue(dataset.getExternalSources()[2].getDatasource().endsWith("costcy2.txt"));
        assertTrue(dataset.getExternalSources()[3].getDatasource().endsWith("does not exist"));
        
        ExternalFilesExporter exporter = new ExternalFilesExporter(dataset, localDbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset), optimizedBatchSize);
        File exportfile = File.createTempFile("ExternalExported", ".txt");
        
        try {
            exporter.export(exportfile);
        } catch (Exception e) {
            assertEquals(3, exporter.getExportedLinesCount());
            assertTrue(e.getMessage().contains("The file " + folder.getAbsolutePath() + File.separator + "does not exist doesn't exist."));
        }
    }
    
}
