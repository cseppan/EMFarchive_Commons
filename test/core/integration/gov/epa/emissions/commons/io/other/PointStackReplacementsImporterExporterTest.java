package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.File;
import java.util.Random;

public class PointStackReplacementsImporterExporterTest extends PersistenceTestCase {
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

    public void testExportPointStackReplacementsData() throws Exception {
        File folder = new File("test/data/other");
        PointStackReplacementsImporter importer = new PointStackReplacementsImporter(folder, new String[]{"pstk.m3.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();

        PointStackReplacementsExporter exporter = new PointStackReplacementsExporter(dataset, datasource, sqlDataTypes);
        File exportfile = File.createTempFile("StackReplacementsExported", ".txt");
        exporter.setDelimiter(",");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(104, countRecords());
    }

    public void testExportVersionedPointStackReplacementsData() throws Exception {
        File folder = new File("test/data/other");
        PointStackReplacementsImporter importer = new PointStackReplacementsImporter(folder, new String[]{"pstk.m3.txt"},
                dataset, datasource, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, datasource);
        importerv.run();

        PointStackReplacementsExporter exporter = new PointStackReplacementsExporter(dataset, datasource, 
                sqlDataTypes, new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("StackReplacementsExported", ".txt");
        exporter.setDelimiter(",");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(104, countRecords());
    }
    
    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
}
