package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

public class GridCrossReferenceExporterTest extends PersistenceTestCase {

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

    public void testExportGridCrossRefData() throws Exception {
        File folder = new File("test/data/spatial");
        GridCrossReferenceImporter importer = new GridCrossReferenceImporter(folder, new String[]{"amgref.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();

        GridCrossReferenceExporter exporter = new GridCrossReferenceExporter(dataset, dbServer, sqlDataTypes);
        File file = File.createTempFile("GridCrossRefExported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(22, countRecords());
    }
    
    public void testExportVersionedGridCrossRefData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/spatial");
        GridCrossReferenceImporter importer = new GridCrossReferenceImporter(folder, new String[]{"amgref.txt"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();
        
        GridCrossReferenceExporter exporter = new GridCrossReferenceExporter(dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(version));
        File file = File.createTempFile("GridCrossRefExported", ".txt");
        exporter.export(file);
        // FIXME: compare the original file and the exported file.
        assertEquals(22, countRecords());
    }

    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
