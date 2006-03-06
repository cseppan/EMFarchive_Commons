package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.VersionedImporter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IDAExImporterTest extends PersistenceTestCase {

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

    public void testShouldImportASmallAreaFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "small-area.txt" };
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(folder, fileNames, dataset, dbServer,
                sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldExportASmallAreaFile() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/ida");
        String[] fileNames = { "small-area.txt" };
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(folder, fileNames, dataset, dbServer,
                sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
        
        IDANonPointNonRoadFileFormat fileFormat = new IDANonPointNonRoadFileFormat(sqlDataTypes);
        fileFormat.addPollutantCols(getPollutantCols(dataset));
        IDANonPointNonRoadExporter exporter = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat);
        File exportfile = File.createTempFile("IDAAreaExported", ".txt");
        exporter.export(exportfile);
        
        String data3 = "37 1 2102006000 5.7519000e+000 2.0600000e-002 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 3.9944100e+001 1.4300000e-001 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 2.6842500e+001 9.6100000e-002 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "3.0100000e-002 1.0000000e-004 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "1.4430000e-001 6.0000000e-004 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "1.3520000e-001 5.0000000e-004 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "4.0749000e+000 2.3600000e-002 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000";
        String data10 = "37 1 2104002000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 8.7829000e+000 1.6000000e-003 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 8.4140000e-001 2.0000000e-004 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 2.9460000e-001 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000";
        List data = readData(exportfile);
        assertEquals(data3, data.get(2));
        assertEquals(data10, data.get(9));
    }

    public void testShouldImportASmallPointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "small-point.txt" };
        IDAPointImporter importer = new IDAPointImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();

        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldExportASmallPointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "small-point.txt" };
        IDAPointImporter importer = new IDAPointImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();

        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
        
        IDAPointFileFormat fileFormat = new IDAPointFileFormat(sqlDataTypes);
        fileFormat.addPollutantCols(getPollutantCols(dataset));
        IDAPointExporter exporter = new IDAPointExporter(dataset, dbServer, fileFormat);
        File exportfile = File.createTempFile("IDAPointExported", ".txt");
        exporter.export(exportfile);
        
        String data1 = "37 1 0010 001 001   01 BFI WASTE SYSTEMS OF NORTH AMERICA, INC. " +
                "50300505 0 0 8.2000000e+001 2.5000000e+000 1.6500000e+002 2.0126000e+002 " +
                "4.1000000e+001 0.0000000e+000  2.5000000e+001 2.5000000e+001 2.5000000e+001 " +
                "2.5000000e+001 24 0 7 1 1.1836000e+004 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 4953 3.6040000e+001 7.9400000e+001  8.4000000e-001 " +
                "2.3000000e-003 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 2.1980000e+001 6.0300000e-002 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 3.8200000e+000 1.0400000e-002 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "1.5400000e+000 4.2000000e-003 6.0000000e+001 0.0000000e+000 0.0000000e+000 " +
                "2.0000000e+000 0.0000000e+000 1.6740000e+001 4.5900000e-002 6.0000000e+001 " +
                "0.0000000e+000 0.0000000e+000 2.0000000e+000 0.0000000e+000 1.4887400e+001 " +
                "4.0900000e-002 6.0000000e+001 0.0000000e+000 0.0000000e+000 2.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000";
        String data10 = "37 1 0034 006 001   02 COPLAND, INC. 10200501 0 0 4.0000000e+001 3.0000000e+000 " +
                "3.7000000e+002 1.1805000e+002 1.6700000e+001 0.0000000e+000  3.0000000e+001 2.5000000e+001 " +
                "2.0000000e+001 2.5000000e+001 24 0 5 1 2.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "5.0000000e-001 0.0000000e+000 0.0000000e+000 2221 3.6120800e+001 7.9404200e+001  " +
                "2.0000000e-004 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 2.4900000e-002 1.0000000e-004 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 6.2000000e-003 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 8.9400000e-002 2.0000000e-004 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 1.2000000e-003 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 3.0000000e-004 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 " +
                "0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000 0.0000000e+000"; 
        List data = readData(exportfile);
        assertEquals(data1, data.get(0));
        assertEquals(data10, data.get(9));
    }

    public void testShouldImportASmallMobileFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "small-mobile.txt" };
        IDAMobileImporter importer = new IDAMobileImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldExportASmallMobileFile() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/ida");
        String[] fileNames = { "small-mobile.txt" };
        IDAMobileImporter importer = new IDAMobileImporter(folder, fileNames, dataset, dbServer, sqlDataTypes,
                new VersionedDataFormatFactory(version));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
        
        IDAMobileFileFormat fileFormat = new IDAMobileFileFormat(sqlDataTypes);
        fileFormat.addPollutantCols(getPollutantCols(dataset));
        IDAMobileExporter exporter = new IDAMobileExporter(dataset, dbServer, fileFormat);
        File exportfile = File.createTempFile("IDAMobileExported", ".txt");
        exporter.export(exportfile);
        
        String data1 = "1 1 0 2201001110 4.6224000e+001 1.3300000e-001 8.1371000e+001 2.3340000e-001 " +
                "6.3896500e+002 1.6686000e+000 2.2250000e+000 7.0000000e-003 9.7400000e-001 3.1000000e-003 " +
                "5.8400000e-001 1.8000000e-003 2.7179000e+000 8.6000000e-003";
        String data10 = "1 1 0 2201001290 1.0148700e+002 2.6390000e-001 6.7366000e+001 1.7750000e-001 " +
                "1.0208800e+003 2.4307000e+000 2.9050000e+000 8.4000000e-003 1.2710000e+000 3.7000000e-003 " +
                "7.2600000e-001 2.1000000e-003 3.5515000e+000 1.1000000e-002";
        List data = readData(exportfile);
        assertEquals(data1, data.get(0));
        assertEquals(data10, data.get(9));
    }

    public void testShouldImportExportASmallActivityFile() throws ImporterException, ExporterException {
        try {
            File folder = new File("test/data/ida");
            String[] fileNames = { "small-activity.txt" };
            IDAActivityImporter importer = new IDAActivityImporter(folder, fileNames, dataset, dbServer,
                    sqlDataTypes);
            importer.run();

            // assert
            Datasource datasource = dbServer.getEmissionsDatasource();
            TableReader tableReader = tableReader(datasource);
            assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));
            
            // Test exporter
            NonVersionedDataFormatFactory factory = new NonVersionedDataFormatFactory();
            IDAActivityFileFormat fileFormat = new IDAActivityFileFormat(sqlDataTypes, factory.defaultValuesFiller());
            File exportfile;
            try {
                exportfile = File.createTempFile("IDAActivity", ".txt");
            } catch (IOException e1) {
                throw new ImporterException("Can't create temp file IDAActivity.txt");
            }
            fileFormat.addPollutantCols(getPollutantCols(dataset));
            IDAActivityExporter exporter = new IDAActivityExporter(dataset, dbServer, fileFormat);
            exporter.export(exportfile);
            String data1 = "37 1 0 2201001150 4.0000000e+001 4.1420000e+001";
            String data10 = "37 1 0 2201020150 4.0000000e+001 1.6770000e+001";
            String pollutant = "#DATA       SPEED VMT";
            try {
                List data = readData(exportfile);
                assertEquals(data1, data.get(0));
                assertEquals(data10, data.get(9));
                assertEquals(pollutant, readComments(exportfile).get(7));
            } catch (IOException e) {
                throw new ImporterException("Can't make assertion.");
            }
        } catch (ImporterException e) {
            throw e;
        } catch (ExporterException e) {
            throw e;
        }
        
    }
    
    private String[] getPollutantCols(Dataset dataset) throws ImporterException {
        try {
            File header = File.createTempFile(dataset.getName(), ".txt");
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(header)));
            writer.write(dataset.getDescription());
            writer.close();
            IDAHeaderReader headerReader = new IDAHeaderReader(header);
            headerReader.read();
            headerReader.close();
            return headerReader.polluntants();
        } catch (IOException e) {
            throw new ImporterException("Can't read header file");
        }
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

    private List readComments(File file) throws IOException {
        List lines = new ArrayList();

        BufferedReader r = new BufferedReader(new FileReader(file));
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            if (isNotEmpty(line) && isComment(line))
                lines.add(line);
        }

        return lines;
    }

    private boolean isComment(String line) {
        return line.startsWith("#");
    }


}
