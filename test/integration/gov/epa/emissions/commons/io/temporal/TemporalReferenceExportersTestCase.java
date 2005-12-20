package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.SimpleTableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

import java.io.File;
import java.util.Random;

//Remove 'abstract' modifier, and remove 'TestCase' suffix to run the test
public abstract class TemporalReferenceExportersTestCase extends PersistenceTestCase {
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

        PointTemporalReferenceFileFormat base = new PointTemporalReferenceFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat = new SimpleTableFormatWithOptionalCols(base, sqlDataTypes);
        createTable("POINT_SOURCE", datasource, tableFormat);

        AreaTemporalReferenceFileFormat base1 = new AreaTemporalReferenceFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat1 = new SimpleTableFormatWithOptionalCols(base1, sqlDataTypes);
        createTable("AREA_SOURCE", datasource, tableFormat1);

        MobileTemporalReferenceFileFormat base2 = new MobileTemporalReferenceFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat2 = new SimpleTableFormatWithOptionalCols(base2, sqlDataTypes);
        createTable("MOBILE_SOURCE", datasource, tableFormat2);
    }

    protected void tearDown() throws Exception {
        dropTable("POINT_SOURCE", datasource);
        dropTable("AREA_SOURCE", datasource);
        dropTable("MOBILE_SOURCE", datasource);
    }

    public void testShouldExportAFileWithVariableCols() throws Exception {
        File file = new File("test/data/temporal-crossreference", "point-source-VARIABLE-COLS.txt");
        PointTemporalReferenceImporter importer = new PointTemporalReferenceImporter(file, dataset, datasource,
                sqlDataTypes);
        importer.run();

        FIX_THE_TEST_PointTemporalReferenceExporter exporter = new FIX_THE_TEST_PointTemporalReferenceExporter(dataset,
                datasource, sqlDataTypes);
        File exportfile = new File("test/data/temporal-crossreference", "point-cross-ref-exported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(20, countRecords("POINT_SOURCE"));
        exportfile.delete();
    }

    public void testShouldExportAreaReferenceData() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        AreaTemporalReferenceImporter importer = new AreaTemporalReferenceImporter(file, dataset, datasource,
                sqlDataTypes);
        importer.run();

        assertEquals(28, countRecords("AREA_SOURCE"));

        AreaTemporalReferenceExporter exporter = new AreaTemporalReferenceExporter(dataset, datasource, sqlDataTypes);
        File exportfile = new File("test/data/temporal-crossreference", "area-cross-ref-exported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(28, countRecords("AREA_SOURCE"));
        exportfile.delete();
    }

    public void testShouldExportMobileReferenceData() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        MobileTemporalReferenceImporter importer = new MobileTemporalReferenceImporter(file, dataset, datasource,
                sqlDataTypes);
        importer.run();

        assertEquals(28, countRecords("MOBILE_SOURCE"));

        MobileTemporalReferenceExporter exporter = new MobileTemporalReferenceExporter(dataset, datasource,
                sqlDataTypes);
        File exportfile = new File("test/data/temporal-crossreference", "mobile-cross-ref-exported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        assertEquals(28, countRecords("MOBILE_SOURCE"));
        exportfile.delete();
    }

    private int countRecords(String table) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), table);
    }

}
