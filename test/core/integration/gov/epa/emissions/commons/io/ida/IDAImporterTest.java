package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.ExporterException;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

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

public class IDAImporterTest extends PersistenceTestCase {

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
            File header = File.createTempFile("IDAHeader", ".txt");
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
