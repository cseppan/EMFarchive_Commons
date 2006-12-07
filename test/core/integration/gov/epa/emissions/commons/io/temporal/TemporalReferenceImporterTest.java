package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TemporalReferenceImporterTest extends PersistenceTestCase {

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

        dbUpdate.deleteAll(datasource.getName(), "versions");
    }

    public void testShouldImportLargeFileWithInlineComments() throws Exception {
        File file = new File("test/data/temporal-crossreference", "amptref.m3.us+can.txt");
        TemporalReferenceImporter importer = new TemporalReferenceImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(20944, rows);

    }

    public void testShouldImportReferenceFile() throws Exception {
        File file = new File("test/data/temporal-crossreference", "areatref.txt");
        TemporalReferenceImporter importer = new TemporalReferenceImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(34, rows);

        TemporalReferenceExporter exporter = new TemporalReferenceExporter(dataset, dbServer, sqlDataTypes,
                optimizedBatchSize);
        File exportfile = File.createTempFile("VersionedCrossRefExported", ".txt");
        exporter.export(exportfile);

        List data = readData(exportfile);
        String data1 = "0000000000;262;7;24;;;;;;;;";
        String data5 = "10100202;262;8;33;;;;;;;;";
        String data34 = "2201080000;262;7;24;\"DNL__ETHYLBENZN\";;;;;;;";
        assertEquals(data1, data.get(1));
        assertEquals(data5, data.get(5));
        assertEquals(data34, data.get(34));
    }

    private int countRecords() {
        Datasource datasource = dbServer.getEmissionsDatasource();
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
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
