package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.VersionedDataFormatFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class ORLExportersTest extends PersistenceTestCase {

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

    public void testShouldExportOnRoad() throws Exception {
        File folder = new File("test/data/orl/nc");
        Importer importer = new ORLOnRoadImporter(folder, new String[] { "small-onroad.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        Exporter exporter = new ORLOnRoadExporter(dataset, dbServer, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(18, data.size());
        assertEquals("37001, 2201001150,           100414, 1.0626200e+000, -9, -9,     -9,   -9,  -9", (String) data
                .get(0));
        assertEquals("37001, 2201001150,           100425, 2.0263000e-001, -9, -9,     -9,   -9,  -9", (String) data
                .get(1));
    }

    public void testShouldExportOnRoadVersionZero() throws Exception {
        File importFile = new File("test/data/orl/nc", "small-onroad.txt");
        DataFormatFactory formatFactory = new VersionedDataFormatFactory(0);
        Importer importer = new ORLOnRoadImporter(importFile.getParentFile(), new String[] { importFile.getName() },
                dataset, dbServer, sqlDataTypes, formatFactory);
        importer.run();

        Exporter exporter = new ORLOnRoadExporter(dataset, dbServer, sqlDataTypes, formatFactory);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(18, data.size());
        assertEquals("37001, 2201001150,           100414, 1.0626200e+000, -9, -9,     -9,   -9,  -9", (String) data
                .get(0));
        assertEquals("37001, 2201001150,           100425, 2.0263000e-001, -9, -9,     -9,   -9,  -9", (String) data
                .get(1));
    }

    public void testShouldExportNonRoad() throws Exception {
        File folder = new File("test/data/orl/nc");
        Importer importer = new ORLNonRoadImporter(folder, new String[] { "small-nonroad.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        Exporter exporter = new ORLNonRoadExporter(dataset, dbServer, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(16, data.size());
        assertEquals("37001, 2260001010,           100414, 5.6000000e-001, -9, -9, -9, -9, -9,         -9,   -9,  -9,       -9,       -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9",
                (String) data.get(0));
        assertEquals("37001, 2260001010,           100425, 3.0000000e-002, -9, -9, -9, -9, -9,         -9,   -9,  -9,       -9,       -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9",
                (String) data.get(1));
    }

    public void testShouldExportNonPoint() throws Exception {
        File folder = new File("test/data/orl/nc");
        Importer importer = new ORLNonPointImporter(folder, new String[] { "small-nonpoint.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        Exporter exporter = new ORLNonPointExporter(dataset, dbServer, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(6, data.size());
        // regex is used because of precision diff between mysql and postgres
        assertTrue(((String) data.get(0))
                .matches("37001,   10201302,    0,   0107,  2,      0,              246, 3.87296\\d*e-004, -9, -9, -9, -9,   -9,   -9,         -9,   -9,  -9,              -9,       -9,       -9,       -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9"));
        assertTrue(((String) data.get(1))
                .matches("37001,   10201302,    0,   0107,  2,      0,              253, 6.91058\\d*e-004, -9, -9, -9, -9,   -9,   -9,         -9,   -9,  -9,              -9,       -9,       -9,       -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9"));
        

    }

    public void testShouldExportPoint() throws Exception {
        File folder = new File("test/data/orl/nc");
        Importer importer = new ORLPointImporter(folder, new String[] { "small-point.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        Exporter exporter = new ORLPointExporter(dataset, dbServer, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert records
        List records = readData(file);
        assertEquals(10, records.size());

        String expectedPattern = "37119,            0001,            0001,               1,               "
                + "1,                 REXAMINC\\.;CUSTOMDIVISION,   40201301, 02, 01, "
                + "6.00000\\d*e\\+001, "
                + "7.50000\\d*e\\+000, 3.75000\\d*e\\+002, 2.08346\\d*e\\+003, 4.71600\\d*e\\+001, 3083,   0714,      "
                + "0, L, -8.07081\\d*e\\+001, 3.51200\\d*e\\+001, 17,           108883, 9.70414\\d*e\\+000,"
                + " -9, -9, -9, -9, -9,                   -9,     -9,    -9, ,         -9,"
                + "         -9,         -9,   -9,  -9, -9, -9,             -9,"
                + "  -9,  -9,              -9,     -9,  -9,  -9,         -9,         -9,           -9,"
                + "       -9,       -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9, -9";
        String actual = (String) records.get(0);
        
        assertTrue(actual.matches(expectedPattern));
    }

    private void assertComments(File file) throws IOException {
        List comments = readComments(file);
        assertEquals(headers(dataset.getDescription()).size(), comments.size());
    }

    private File doExport(Exporter exporter) throws Exception {
        File file = File.createTempFile("exported", ".orl");
        file.deleteOnExit();

        exporter.export(file);

        return file;
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
