package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVFileExImporterTest extends PersistenceTestCase {
    private Datasource datasource;
    
    private Dataset dataset;

    private SqlDataTypes sqlDataTypes;

    private String tableName;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();
        tableName = "test";
        dataset = new SimpleDataset();
        dataset.setName("test");
        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableName);
    }

    public void testImportASmallAndSimplePointFileWithCSVImporter() throws Exception {
        File folder = new File("test/data/reference");
        Importer importer = new CSVImporter(folder, new String[]{"pollutants.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();
        
        int rows = countRecords();
        assertEquals(8, rows);
        
        File file = File.createTempFile("ExportedSmallAndSimplePointFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, dbServer, sqlDataTypes);
        exporter.export(file);
        
        List data = readData(file);
        assertEquals(data.get(0), "CO;CO");
        assertEquals(data.get(7), "VOC;VOC");
    }
    
    public void testImportASmallAndSimplePointFileWithVersionedCSVImporter() throws Exception {
        File folder = new File("test/data/reference");
        Importer importer = new CSVImporter(folder, new String[]{"pollutants.txt"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        int rows = countRecords();
        assertEquals(8, rows);
        
        File file = File.createTempFile("ExportedSmallAndSimplePointFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        exporter.export(file);
        
        List data = readData(file);
        assertEquals(data.get(0), "CO;CO");
        assertEquals(data.get(7), "VOC;VOC");
    }
    
    public void testShouldExImportSimpleCommaDelimitedCSVFile() throws Exception {
        File folder = new File("test/data/csv");
        Importer importer = new CSVImporter(folder, new String[]{"generation_file.csv"},
                dataset, dbServer, sqlDataTypes);
        importer.run();
        
        int rows = countRecords();
        assertEquals(14, rows);
        
        File file = File.createTempFile("ExportedCommaDelimitedFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, dbServer, sqlDataTypes);
        exporter.export(file);
        
        List data = readData(file);
        assertEquals(data.get(0), "USA;Population;100;NO;YES");
        assertEquals(data.get(6), "USA;3/4 Total Roadway Miles plus 1/4 Population;255;YES;");
    }

    public void testShouldExImportSurrogateSpecFile() throws Exception {
        File folder = new File("test/data/csv");
        Importer importer = new CSVImporter(folder, new String[]{"surrogate_specification.csv"},
                dataset, dbServer, sqlDataTypes);
        importer.run();
        
        int rows = countRecords();
        assertEquals(14, rows);
        
        File file = File.createTempFile("ExportedCommaDelimitedFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, dbServer, sqlDataTypes);
        exporter.export(file);
        
        List data = readData(file);
        String expect = "USA;Population;100;pophu2k;POP2000;;;;;;;" +
                "Total population from Census 2000 blocks;";
        assertEquals(data.get(0), expect);
        expect = "USA;3/4 Total Roadway Miles plus 1/4 Population;255;;;;;"
            + "0.75*Total Road Miles+0.25*Population;Population;;;"
            + "Combination of  3/4 total road miles surrogate ratio and 1/4 population surrogate ratio;";
        assertEquals(data.get(6), expect);
    }
    
    public void testShouldExImportShapeCatFile() throws Exception {
        File folder = new File("test/data/csv");
        Importer importer = new CSVImporter(folder, new String[]{"shapefile_catalog.csv"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();
        
        int rows = countRecords();
        assertEquals(41, rows);
        
        File file = File.createTempFile("ExportedCommaDelimitedFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        exporter.export(file);
        
        List data = readData(file);
        String expect = "cnty_tn_lcc;D:\\MIMS\\mimssp_7_2005\\data\\;SPHERE;" +
                "proj=lcc,+lat_1=33,+lat_2=45,+lat_0=40,+lon_0=_97;" +
                "TN county boundaries;from UNC CEP machine;";
        assertEquals(data.get(0), expect);
        expect = "us_ph;D:\\MIMS\\emiss_shp2003\\us\\;;;The change in housing between 1990 and 2000;US Census Bureau;No Data";
        assertEquals(data.get(6), expect);
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), tableName);
    }
    
    private List readData(File file) throws IOException {
        List data = new ArrayList();

        BufferedReader r = new BufferedReader(new FileReader(file));
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            if (isNotEmpty(line) && !isComment(line))
                data.add(line);
        }

        return data;
    }

    private boolean isNotEmpty(String line) {
        return line.length() != 0;
    }

    private boolean isComment(String line) {
        return line.startsWith("#");
    }
}
