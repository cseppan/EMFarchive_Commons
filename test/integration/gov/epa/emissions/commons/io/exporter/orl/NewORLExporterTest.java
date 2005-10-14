package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.orl.OrlOnRoadColumnsMetadata;
import gov.epa.emissions.commons.io.importer.orl.OrlOnRoadImporter;
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

public class NewORLExporterTest extends DbTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    private NewORLExporter exporter;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getDataType();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(new Random().nextLong());

        doImport();

        ORLColumnsMetadata colsMetadata = new OrlOnRoadColumnsMetadata(sqlDataTypes);
        exporter = new NewORLExporter(dataset, datasource, colsMetadata);
    }

    private void doImport() throws ImporterException {
        File file = new File("test/data/orl/nc/small-onroad.txt");
        OrlOnRoadImporter importer = new OrlOnRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
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

    public void testShouldExportFirstRecordOfTable() throws Exception {
        File file = File.createTempFile("onroad", "orl");
        file.deleteOnExit();

        exporter.export(file);

        // assert headers
        List lines = readData(file);
        assertEquals(18, lines.size());
    }

    private List readData(File file) throws IOException {
        List data = new ArrayList();

        BufferedReader r = new BufferedReader(new FileReader(file));
        for (String line = r.readLine(); line != null; line = r.readLine()) {
            System.out.println(line);
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