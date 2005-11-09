package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewExporter;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class NewORLExportersTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldExportOnRoad() throws Exception {
        Importer importer = new ORLOnRoadImporter(dataset, datasource, sqlDataTypes);
        doImport(importer, "small-onroad.txt");

        NewExporter exporter = new ORLOnRoadExporter(dataset, datasource, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(18, data.size());
        assertEquals("37001, 2201001150,           100414, 1.0626200e+000, -9", (String) data.get(0));
        assertEquals("37001, 2201001150,           100425, 2.0263000e-001, -9", (String) data.get(1));
    }
    
    public void testShouldExportNonRoad() throws Exception {
        Importer importer = new ORLNonRoadImporter(dataset, datasource, sqlDataTypes);
        doImport(importer, "small-nonroad.txt");

        NewExporter exporter = new ORLNonRoadExporter(dataset, datasource, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(16, data.size());
        assertEquals("37001, 2260001010,           100414, 5.6000000e-001, -9, -9, -9, -9", (String) data.get(0));
        assertEquals("37001, 2260001010,           100425, 3.0000000e-002, -9, -9, -9, -9", (String) data.get(1));
    }
    
    public void testShouldExportNonPoint() throws Exception {
        Importer importer = new ORLNonPointImporter(dataset, datasource, sqlDataTypes);
        doImport(importer, "small-nonpoint.txt");

        NewExporter exporter = new ORLNonPointExporter(dataset, datasource, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert data
        List data = readData(file);
        assertEquals(6, data.size());
        assertEquals("37001,   10201302,    0,   0107,  2,      0,              246, 3.8729600e-004, -9, -9, -9, -9",
                (String) data.get(0));
        assertEquals("37001,   10201302,    0,   0107,  2,      0,              253, 6.9105800e-004, -9, -9, -9, -9",
                (String) data.get(1));
    }

    public void testShouldExportPoint() throws Exception {
        Importer importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);
        doImport(importer, "small-point.txt");

        NewExporter exporter = new ORLPointExporter(dataset, datasource, sqlDataTypes);
        File file = doExport(exporter);

        // assert headers
        assertComments(file);

        // assert records
        List records = readData(file);
        assertEquals(10, records.size());
        String expected = "37119,            0001,            0001,               1,               1,"
                + "                 REXAMINC.;CUSTOMDIVISION,   40201301, 02, 01, 6.0000000e+001, "
                + "7.5000000e+000, 3.7500000e+002, 2.0834600e+003, 4.7160000e+001, 3083,   0714,"
                + "      0, L, -8.0708100e+001, 3.5120000e+001, 17,           108883, "
                + "9.7041400e+000, -9, -9, -9, -9, -9";
        String actual = (String) records.get(0);
        assertEquals(expected, actual);
    }
    
    private void assertComments(File file) throws IOException {
        List comments = readComments(file);
        assertEquals(headers(dataset.getDescription()).size(), comments.size());
    }

    private File doExport(NewExporter exporter) throws Exception {
        File file = File.createTempFile("exported", "orl");
        file.deleteOnExit();

        exporter.export(file);

        return file;
    }

    private void doImport(Importer importer, String filename) throws Exception {
        File folder = new File("test/data/orl/nc");
        importer.preCondition(folder, filename);
        importer.run(dataset);
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
