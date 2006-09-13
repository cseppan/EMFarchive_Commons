package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
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

public class PointStackReplacementsImporterExporterTest extends PersistenceTestCase {

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
        dataset.setDatasetType(new DatasetType("dsType"));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportPointStackReplacementsData() throws Exception {
        File folder = new File("test/data/other");
        PointStackReplacementsImporter importer = new PointStackReplacementsImporter(folder, new String[]{"pstk.m3.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();

        PointStackReplacementsExporter exporter = new PointStackReplacementsExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        File exportfile = File.createTempFile("StackReplacementsExported", ".txt");
        exporter.setDelimiter(",");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(104, countRecords());
    }

    public void testExportVersionedPointStackReplacementsData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        PointStackReplacementsImporter importer = new PointStackReplacementsImporter(folder, new String[]{"pstk.m3.txt"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        PointStackReplacementsExporter exporter = new PointStackReplacementsExporter(dataset, dbServer, 
                sqlDataTypes, new VersionedDataFormatFactory(version, dataset),optimizedBatchSize);
        File exportfile = File.createTempFile("StackReplacementsExported", ".txt");
        exporter.setDelimiter(",");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(104, countRecords());
    }
    
    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
