package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Exporter;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class ORLOnRoadExporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private Exporter exporter;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        doImport();

        exporter = new ORLOnRoadExporter(dataset, datasource, sqlDataTypes);
    }

    private void doImport() throws Exception {
        File folder = new File("test/data/orl/nc");
        Importer importer = new ORLOnRoadImporter(folder, new String[] { "small-onroad.txt" }, dataset, datasource,
                sqlDataTypes);
        importer.run();
    }

    protected void doTearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), dataset.getName());
    }

    public void testShouldExportHeadersForOnRoad() throws Exception {
        File file = File.createTempFile("onroad", "orl");
        file.deleteOnExit();

        exporter.export(file);

        // assert headers
        List lines = readComments(file);
        assertEquals(headers(dataset.getDescription()).size(), lines.size());
    }

    // FIXME: ORL import: convert -9 to null?? export convert null -> -9 or emptyspace?
    public void testShouldExportTableRowsAsRecords() throws Exception {
        File file = File.createTempFile("onroad", ".orl");
        file.deleteOnExit();

        exporter.export(file);

        // assert headers
        List lines = readData(file);
        assertEquals(18, lines.size());
        String line = (String) lines.get(0);
        // FIXME: revisit once 'comments' is moved from Table to specific
        // ColumnsMetadata
        // assertEquals("37001, 2201001150, 100414, 1.0626200e+000, -9, !
        // EPA-derived", line);
        assertEquals("37001, 2201001150,           100414, 1.0626200e+000, -9, -9,     -9,   -9,  -9", line);
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
