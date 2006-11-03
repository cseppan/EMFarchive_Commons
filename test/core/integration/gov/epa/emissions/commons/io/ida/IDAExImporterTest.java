package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.ExporterException;
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
import java.util.Date;
import java.util.List;
import java.util.Random;

public class IDAExImporterTest extends PersistenceTestCase {

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private DbServer dbServer;

    private Integer optimizedBatchSize;

    protected void setUp() throws Exception {
        super.setUp();
        dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        optimizedBatchSize = new Integer(10000);
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
                sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,fileNames[0]));
        importer2.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));

        IDANonPointNonRoadFileFormat fileFormat = new IDANonPointNonRoadFileFormat(sqlDataTypes);
        fileFormat.addPollutantCols(getPollutantCols(dataset));
        IDANonPointNonRoadExporter exporter = new IDANonPointNonRoadExporter(dataset, dbServer, fileFormat, optimizedBatchSize);
        File exportfile = File.createTempFile("IDAAreaExported", ".txt");
        exporter.export(exportfile);

        String data3 = "37  12102006000    5.7519    0.0206          0      0  0     0   39.9441     0.143          "
                + "0      0  0     0   26.8425    0.0961          0      0  0     0    0.0301    0.0001          0      "
                + "0  0     0    0.1443    0.0006          0      0  0     0    0.1352    0.0005          0      0  0     "
                + "0    4.0749    0.0236          0      0  0     0";
        String data10 = "37  12104002000         0         0          0      0  0     0         0         0          0      "
                + "0  0     0         0         0          0      0  0     0    8.7829    0.0016          0      0  0     0    "
                + "0.8414    0.0002          0      0  0     0    0.2946         0          0      0  0     0         0         "
                + "0          0      0  0     0";
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
        IDAPointExporter exporter = new IDAPointExporter(dataset, dbServer, fileFormat, optimizedBatchSize);
        File exportfile = File.createTempFile("IDAPointExported", ".txt");
        exporter.export(exportfile);

        String data1 = "37  10010           001            001                     01BFI WASTE SYSTEMS OF NORTH AMERICA, INC."
                + "50300505     0   0  82   2.5 165    201.26       41       0 2525252524 07 1      11836           0       0    0    "
                + "0        04953    36.04     79.4          0.84       0.0023      0  0         0  0  0        21.98       0.0603      "
                + "0  0         0  0  0         3.82       0.0104      0  0         0  0  0         1.54       0.0042     60  "
                + "0         0  2  0        16.74       0.0459     60  0         0  2  0      14.8874       0.0409     60  0         0  "
                + "2  0            0            0      0  0         0  0  0";
        String data10 = "37  10034           006            001                     02COPLAND, INC.                           10200501     "
                + "0   0  40     3 370    118.05     16.7       0 3025202524 05 1          2           0       0  0.5    "
                + "0        02221  36.1208  79.4042        0.0002            0      0  0         0  0  0       0.0249       0.0001      "
                + "0  0         0  0  0       0.0062            0      0  0         0  0  0       0.0894       0.0002      0  0         0  "
                + "0  0       0.0012            0      0  0         0  0  0       0.0003            0      0  0         0  0  0            "
                + "0            0      0  0         0  0  0";
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
                new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer, lastModifiedDate(folder,fileNames[0]));
        importer2.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(10, tableReader.count(datasource.getName(), dataset.getName()));

        IDAMobileFileFormat fileFormat = new IDAMobileFileFormat(sqlDataTypes);
        fileFormat.addPollutantCols(getPollutantCols(dataset));
        IDAMobileExporter exporter = new IDAMobileExporter(dataset, dbServer, fileFormat, optimizedBatchSize);
        File exportfile = File.createTempFile("IDAMobileExported", ".txt");
        exporter.export(exportfile);

        String data1 = " 1  10         2201001110    46.224     0.133    81.371    0.2334   "
                + "638.965    1.6686     2.225     0.007     0.974    0.0031     0.584    0.0018    2.7179    0.0086";
        String data10 = " 1  10         2201001290   101.487    0.2639    67.366    0.1775  1020.877    2.4307     "
                + "2.905    0.0084     1.271    0.0037     0.726    0.0021    3.5515     0.011";
        List data = readData(exportfile);
        assertEquals(data1, data.get(0));
        assertEquals(data10, data.get(9));
    }

    public void testShouldImportExportASmallActivityFile() throws ImporterException, ExporterException {
        try {
            File folder = new File("test/data/ida");
            String[] fileNames = { "small-activity.txt" };
            IDAActivityImporter importer = new IDAActivityImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
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
            IDAActivityExporter exporter = new IDAActivityExporter(dataset, dbServer, fileFormat, optimizedBatchSize);
            exporter.export(exportfile);
            String data1 = "37 1 0 2201001150 40 41.42";
            String data10 = "37 1 0 2201020150 40 16.77";
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
    
    
    public void testShouldImportACanadaNonpointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "arinv.ca96_v3_stat.ida" };
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(38, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportACanadaPointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "ptinv.ca96_v1_pnt.ida" };
        IDAPointImporter importer = new IDAPointImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(41, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    
    public void testShouldImportAMexicoPointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "IDA-MexicoBorderPoint_20051220.txt" };
        IDAPointImporter importer = new IDAPointImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(748, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportAMexicoNonPointFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "IDA-nonpoint-MX-BorderStates-20051027.txt" };
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(9181, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportAMexicoNonRoadFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "IDA-MexicoBorderNonroad_20051220.txt" };
        IDANonPointNonRoadImporter importer = new IDANonPointNonRoadImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(514, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    public void testShouldImportAMexicoOnRoadFile() throws Exception {
        File folder = new File("test/data/ida");
        String[] fileNames = { "IDA-onroad-MX-BorderStates-20051021.txt" };
        IDAMobileImporter importer = new IDAMobileImporter(folder, fileNames, dataset, dbServer, sqlDataTypes);
        importer.run();
        // assert
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        assertEquals(1932, tableReader.count(datasource.getName(), dataset.getName()));
    }
    
    private Date lastModifiedDate(File folder, String fileName) {
        return new Date(new File(folder,fileName).lastModified());
    }
}
