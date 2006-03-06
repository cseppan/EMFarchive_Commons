package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class CountryStateCountyDataExporterTest extends PersistenceTestCase {

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
        dbUpdate.dropTable(datasource.getName(), "country");
        dbUpdate.dropTable(datasource.getName(), "state");
        dbUpdate.dropTable(datasource.getName(), "county");
    }

    public void testExportCountryStateCountyData() throws Exception {
        File folder = new File("test/data/other");
        CountryStateCountyDataImporter importer = new CountryStateCountyDataImporter(folder, new String[]{"costcy.txt"},
                dataset, dbServer, sqlDataTypes);
        importer.run();

        CountryStateCountyDataExporter exporter = new CountryStateCountyDataExporter(dataset, 
                dbServer, sqlDataTypes);
        File file = File.createTempFile("CSCexported", ".txt");
        exporter.export(file);
        
        // assert headers
        assertComments(file);
        
        // assert data
        List data = readData(file);
        assertEquals(37, data.size());
        assertEquals("/COUNTRY/", (String) data.get(0));
        assertEquals("0  US", ((String) data.get(1)).trim());
        assertEquals("/STATE/", (String) data.get(8));
        assertEquals("/COUNTY/", (String) data.get(20));
    }
    
    public void testExportVersionedCountryStateCountyData() throws Exception {
        Version version = new Version();
        version.setVersion(0);

        File folder = new File("test/data/other");
        CountryStateCountyDataImporter importer = new CountryStateCountyDataImporter(folder, new String[]{"costcy.txt"},
                dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        VersionedImporter importerv = new VersionedImporter(importer, dataset, dbServer);
        importerv.run();

        CountryStateCountyDataExporter exporter = new CountryStateCountyDataExporter(dataset, 
                dbServer, sqlDataTypes, new VersionedDataFormatFactory(version));
        File file = File.createTempFile("CSCexported", ".txt");
        exporter.export(file);
        
        // assert headers
        assertComments(file);
        
        // assert data
        List data = readData(file);
        assertEquals(37, data.size());
        assertEquals("/COUNTRY/", (String) data.get(0));
        assertEquals("0  US", ((String) data.get(1)).trim());
        assertEquals("/STATE/", (String) data.get(8));
        assertEquals("/COUNTY/", (String) data.get(20));
    }
    
    private void assertComments(File file) throws IOException {
        List comments = readComments(file);
        assertEquals(headers(dataset.getDescription()).size(), comments.size());
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

    private List headers(String description) {
        List headers = new ArrayList();
        Pattern p = Pattern.compile("\n");
        String[] tokens = p.split(description);
        headers.addAll(Arrays.asList(tokens));

        return headers;
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
