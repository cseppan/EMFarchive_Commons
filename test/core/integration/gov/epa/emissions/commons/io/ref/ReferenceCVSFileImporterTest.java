package gov.epa.emissions.commons.io.ref;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.csv.CSVExporter;
import gov.epa.emissions.commons.io.csv.CSVImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;
import gov.epa.emissions.commons.io.reference.ReferenceCSVFileImporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceCVSFileImporterTest extends PersistenceTestCase {

    private Datasource datasource;
    
    private Dataset dataset;

    private SqlDataTypes sqlDataTypes;

    private String tableName;

    protected void setUp() throws Exception {
        super.setUp();
        tableName = "test";
        dataset = new SimpleDataset();
        dataset.setName("test");
        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), tableName);
    }

    public void testShouldImportASmallAndSimplePointFile() throws Exception {
        File file = new File("test/data/reference", "pollutants.txt");

        Importer importer = new ReferenceCSVFileImporter(file, tableName, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(8, rows);
    }
    
    public void testImportASmallAndSimplePointFileWithCSVImporter() throws Exception {
        File folder = new File("test/data/reference");
        Importer importer = new CSVImporter(folder, new String[]{"pollutants.txt"},
                dataset, datasource, sqlDataTypes);
        importer.run();
        
        int rows = countRecords();
        assertEquals(8, rows);
        
        File file = File.createTempFile("ExportedSmallAndSimplePointFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, datasource, sqlDataTypes);
        exporter.export(file);
        
        List data = readData(file);
        assertEquals(data.get(0), "CO;CO");
        assertEquals(data.get(7), "VOC;VOC");
    }
    
    public void testImportASmallAndSimplePointFileWithVersionedCSVImporter() throws Exception {
        File folder = new File("test/data/reference");
        Importer importer = new CSVImporter(folder, new String[]{"pollutants.txt"},
                dataset, datasource, sqlDataTypes, new VersionedDataFormatFactory(0));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, datasource);
        importerv.run();

        int rows = countRecords();
        assertEquals(8, rows);
        
        File file = File.createTempFile("ExportedSmallAndSimplePointFile", ".txt");
        CSVExporter exporter = new CSVExporter(dataset, datasource, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        exporter.export(file);
        
        List data = readData(file);
        assertEquals(data.get(0), "CO;CO");
        assertEquals(data.get(7), "VOC;VOC");
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
