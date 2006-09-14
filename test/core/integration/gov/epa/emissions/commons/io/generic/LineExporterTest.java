package gov.epa.emissions.commons.io.generic;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.data.DatasetType;
import gov.epa.emissions.commons.data.SimpleDataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
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

    public void testExportSmallLineFile() throws Exception {
        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[] { "small-point.txt" }, dataset, dbServer,
                sqlDataTypes);
        importer.run();

        LineExporter exporter = new LineExporter(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        File file = File.createTempFile("lineexporter", ".txt");
        exporter.export(file);
        assertEquals(22, countRecords());

        // assert records
        List records = readData(file);

        String expectedPattern1 = "#ORL";
        String expectedPattern2 = "37119 0001 0001 1 1 'REXAMINC.;CUSTOMDIVISION' 40201301 02 01 60 7.5 375 2083.463 47.16 3083 0714 0 L -80.7081 35.12 17 108883 9.704141 -9 -9 -9 -9 -9!inline conmments  wihout delimitter separating";
        String expectedPattern3 = "#Cutomized Division - 1998";
        String expectedPattern4 = "#End Comment";

        assertEquals(expectedPattern1, records.get(0));
        assertEquals(expectedPattern2, records.get(6));
        assertEquals(expectedPattern3, records.get(15));
        assertEquals(expectedPattern4, records.get(21));
    }

    public void testExportVersionedSmallLineFile() throws Exception {
        Version version = version();
        File folder = new File("test/data/orl/nc");
        LineImporter importer = new LineImporter(folder, new String[] { "small-point.txt" }, dataset, dbServer,
                sqlDataTypes, new VersionedDataFormatFactory(version, dataset));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();

        LineExporter exporter = new LineExporter(dataset, dbServer, sqlDataTypes, new VersionedDataFormatFactory(
                version, dataset), optimizedBatchSize);
        File file = File.createTempFile("lineexporter", ".txt");
        exporter.export(file);
        assertEquals(22, countRecords());

        // assert records
        List records = readData(file);

        String expectedPattern1 = "#ORL";
        String expectedPattern2 = "37119 0001 0001 1 1 'REXAMINC.;CUSTOMDIVISION' 40201301 02 01 60 7.5 375 2083.463 47.16 3083 0714 0 L -80.7081 35.12 17 108883 9.704141 -9 -9 -9 -9 -9!inline conmments  wihout delimitter separating";
        String expectedPattern3 = "#Cutomized Division - 1998";
        String expectedPattern4 = "#End Comment";

        assertEquals(expectedPattern1, records.get(0));
        assertEquals(expectedPattern2, records.get(6));
        assertEquals(expectedPattern3, records.get(15));
        assertEquals(expectedPattern4, records.get(21));
    }

    private Version version() {
        Version version = new Version();
        version.setVersion(0);
        version.setDatasetId(dataset.getId());
        return version;
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
