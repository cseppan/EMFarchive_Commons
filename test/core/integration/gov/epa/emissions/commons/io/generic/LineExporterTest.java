package gov.epa.emissions.commons.io.generic;

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LineExporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testExportSmallLineFile() throws Exception {
        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[]{"small-point.txt"}, dataset, dbServer, sqlDataTypes);
        importer.run();

        LineExporter exporter = new LineExporter(dataset, dbServer, sqlDataTypes);
        File file = File.createTempFile("lineexporter", ".txt");
        exporter.export(file);
        assertEquals(22, countRecords());
        
        // assert records
        List records = readData(file);

        String expectedPattern1 = "#ORL";
        String expectedPattern2 = "37119 0001 0001 1 1 'REXAMINC.;CUSTOMDIVISION' 40201301 02 01 60 7.5 375 2083.463 47.16 3083 0714 0 L _80.7081 35.12 17 108883 9.704141 _9 _9 _9 _9 _9";
        String expectedPattern3 = "#Cutomized Division _ 1998";
        String expectedPattern4 = "#End Comment";

        assertEquals(expectedPattern1, records.get(0));
        assertEquals(expectedPattern2, records.get(6));
        assertEquals(expectedPattern3, records.get(15));
        assertEquals(expectedPattern4, records.get(21));
    }
    
    public void testExportVersionedSmallLineFile() throws Exception {
        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[]{"small-point.txt"}, dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(0));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();

        LineExporter exporter = new LineExporter(dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(0));
        File file = new File("C:\\lineexporter.txt");
        exporter.export(file);
        assertEquals(22, countRecords());
        
        // assert records
        List records = readData(file);

        String expectedPattern1 = "#ORL";
        String expectedPattern2 = "37119 0001 0001 1 1 'REXAMINC.;CUSTOMDIVISION' 40201301 02 01 60 7.5 375 2083.463 47.16 3083 0714 0 L _80.7081 35.12 17 108883 9.704141 _9 _9 _9 _9 _9";
        String expectedPattern3 = "#Cutomized Division _ 1998";
        String expectedPattern4 = "#End Comment";

        assertEquals(expectedPattern1, records.get(0));
        assertEquals(expectedPattern2, records.get(6));
        assertEquals(expectedPattern3, records.get(15));
        assertEquals(expectedPattern4, records.get(21));
    }

    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }
    
    private List readData(File file) throws IOException {
        List data = new ArrayList();

        BufferedReader r = new BufferedReader(new FileReader(file));
        for (String line = r.readLine(); line != null; line = r.readLine())
            data.add(line);

        return data;
    }
    
}
