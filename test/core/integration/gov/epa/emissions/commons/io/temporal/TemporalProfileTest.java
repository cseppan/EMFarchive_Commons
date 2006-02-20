package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
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

public class TemporalProfileTest extends PersistenceTestCase {

    private SqlDataTypes typeMapper;

    private TableReader tableReader;

    private Dataset dataset;

    private DbServer dbServer;

    protected void setUp() throws Exception {
        super.setUp();

        dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getSqlDataTypes();
        tableReader = tableReader(dbServer.getEmissionsDatasource());
        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setId(Math.abs(new Random().nextInt()));
    }

    protected void doTearDown() throws Exception {
        Datasource datasource = dbServer.getEmissionsDatasource();
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        String schema = datasource.getName();

        if (tableReader.exists(schema, "Monthly"))
            dbUpdate.dropTable(schema, "Monthly");
        if (tableReader.exists(schema, "Weekly"))
            dbUpdate.dropTable(schema, "Weekly");
        if (tableReader.exists(schema, "Diurnal_Weekday"))
            dbUpdate.dropTable(schema, "Diurnal_Weekday");
        if (tableReader.exists(schema, "Diurnal_Weekend"))
            dbUpdate.dropTable(schema, "Diurnal_Weekend");

        dbUpdate.deleteAll(schema, "versions");
    }

    public void testShouldImportTwoProfileFilesSuccessively() throws ImporterException {
        runProfileImporter("small.txt");
        assertEquals(10, countRecords("Monthly"));
        assertEquals(13, countRecords("WEEKLY"));
        assertEquals(20, countRecords("DIURNAL_WEEKEND"));
        assertEquals(20, countRecords("DIURNAL_WEEKDAY"));
        
        runProfileImporter("small.txt");
        assertEquals(20, countRecords("Monthly"));
        assertEquals(26, countRecords("WEEKLY"));
        assertEquals(40, countRecords("DIURNAL_WEEKEND"));
        assertEquals(40, countRecords("DIURNAL_WEEKDAY"));
    }

    private void runProfileImporter(String fileName) throws ImporterException {
        String folder = "test/data/temporal-profiles";
        File file = new File(folder, fileName);
        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();
    }

    public void testShouldReadFromFileAndLoadSamllPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/small.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();

        // assert
        assertEquals(10, countRecords("Monthly"));
        assertEquals(13, countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper);
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(63, records.size());

        String expectedPattern1 = "    1     0     0     0     0     0   110   110"
                + "   110   223   223   223     0   999";
        String expectedPattern2 = "    8   147   147   147   147   147   135   129  1000";
        String expectedPattern3 = " 2006    88    49    33    24    36   119   332"
                + "   854   588   493   485   520   535   557   648   710   789   867"
                + "   660   456   387   338   257   176  1000";
        String expectedPattern4 = " 2001   166   122   103    87    92   120   182"
                + "   263   367   501   623   697   721   738   750   752   751   697"
                + "   584   480   400   331   272   201  1000";

        assertEquals((String) records.get(0), expectedPattern1);
        assertEquals((String) records.get(17), expectedPattern2);
        assertEquals((String) records.get(38), expectedPattern3);
        assertEquals((String) records.get(53), expectedPattern4);
    }

    public void testShouldImportExportVersionedSamllPacketData() throws Exception {
        File file = new File("test/data/temporal-profiles/small.txt");

        TemporalProfileImporter tempProImporter = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper, new VersionedDataFormatFactory(0));
        VersionedImporter importer = new VersionedImporter(tempProImporter, dataset, dbServer);
        importer.run();

        // assert
        assertEquals(10, countRecords("Monthly"));
        assertEquals(13, countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper,
                new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(63, records.size());

        String expectedPattern1 = "    1     0     0     0     0     0   110   110"
                + "   110   223   223   223     0   999";
        String expectedPattern2 = "    8   147   147   147   147   147   135   129  1000";
        String expectedPattern3 = " 2006    88    49    33    24    36   119   332"
                + "   854   588   493   485   520   535   557   648   710   789   867"
                + "   660   456   387   338   257   176  1000";
        String expectedPattern4 = " 2001   166   122   103    87    92   120   182"
                + "   263   367   501   623   697   721   738   750   752   751   697"
                + "   584   480   400   331   272   201  1000";

        assertEquals((String) records.get(0), expectedPattern1);
        assertEquals((String) records.get(17), expectedPattern2);
        assertEquals((String) records.get(38), expectedPattern3);
        assertEquals((String) records.get(53), expectedPattern4);
    }

    public void testShouldReadFromFileAndLoadDiurnalWeekdayPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/diurnal-weekday.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();

        // assert
        assertEquals(20, countRecords("DIURNAL_WEEKDAY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper);
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(20, records.size());

        String expectedPattern = "    1     0     0     0     0     0     0     0     0   125   125"
                + "   125   125   125   125   125   125     0     0     0     0     0     0     0     0  1000";

        String actual = (String) records.get(0);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldImportExportVersionedDiurnalWeekdayPacketData() throws Exception {
        File file = new File("test/data/temporal-profiles/diurnal-weekday.txt");

        TemporalProfileImporter tempProImporter = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper, new VersionedDataFormatFactory(0));
        VersionedImporter importer = new VersionedImporter(tempProImporter, dataset, dbServer);
        importer.run();

        // assert
        assertEquals(20, countRecords("DIURNAL_WEEKDAY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper,
                new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(20, records.size());

        String expectedPattern = "    1     0     0     0     0     0     0     0     0   125   125"
                + "   125   125   125   125   125   125     0     0     0     0     0     0     0     0  1000";

        String actual = (String) records.get(0);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldReadFromFileAndLoadDiurnalWeekendPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/diurnal-weekend.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();

        // assert
        assertEquals(20, countRecords("DIURNAL_WEEKEND"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper);
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(20, records.size());

        String expectedPattern = "   10     0     0     0     0     0     0     0   100   100   100   100   100   100   100   100   100   100     0     0     0     0     0     0     0  1000";

        String actual = (String) records.get(9);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldImportExportVersionedDiurnalWeekendPacketData() throws Exception {
        File file = new File("test/data/temporal-profiles/diurnal-weekend.txt");

        TemporalProfileImporter tempProImporter = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper, new VersionedDataFormatFactory(0));
        VersionedImporter importer = new VersionedImporter(tempProImporter, dataset, dbServer);
        importer.run();

        // assert
        assertEquals(20, countRecords("DIURNAL_WEEKEND"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper,
                new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(20, records.size());

        String expectedPattern = "   10     0     0     0     0     0     0     0   100   100   100   100   100   100   100   100   100   100     0     0     0     0     0     0     0  1000";

        String actual = (String) records.get(9);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldReadFromFileAndLoadMonthlyPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/monthly.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();

        // assert
        assertEquals(10, countRecords("MONTHLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper);
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(10, records.size());

        String expectedPattern = "   10    32    32    12    12    12    31    31    31   258   258   258    32   999";

        String actual = (String) records.get(9);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldImportExportVersionedMonthlyPacketData() throws Exception {
        File file = new File("test/data/temporal-profiles/monthly.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper, new VersionedDataFormatFactory(0));
        VersionedImporter importer2 = new VersionedImporter(importer, dataset, dbServer);
        importer2.run();

        // assert
        assertEquals(10, countRecords("MONTHLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper,
                new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(10, records.size());

        String expectedPattern = "   10    32    32    12    12    12    31    31    31   258   258   258    32   999";

        String actual = (String) records.get(9);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldReadFromFileAndLoadWeeklyPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/weekly.txt");

        TemporalProfileImporter importer = new TemporalProfileImporter(file.getParentFile(), new String[] { file
                .getName() }, dataset, dbServer, typeMapper);
        importer.run();

        // assert
        assertEquals(13, countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper);
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(13, records.size());

        String expectedPattern = "    6   167   167   167   167   167   167     0  1000";

        String actual = (String) records.get(5);
        assertTrue(actual.matches(expectedPattern));
    }

    public void testShouldImportExportVersionedWeeklyPacket() throws Exception {
        File file = new File("test/data/temporal-profiles/weekly.txt");

        TemporalProfileImporter temporalProfileImporter = new TemporalProfileImporter(file.getParentFile(),
                new String[] { file.getName() }, dataset, dbServer, typeMapper, new VersionedDataFormatFactory(0));
        VersionedImporter importer = new VersionedImporter(temporalProfileImporter, dataset, dbServer);
        importer.run();

        // assert
        assertEquals(13, countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, dbServer, typeMapper,
                new VersionedDataFormatFactory(0));
        File exportfile = File.createTempFile("VersionedTemporalProfileExported", ".txt");
        exporter.export(exportfile);

        // assert records
        List records = readData(exportfile);
        assertEquals(13, records.size());

        String expectedPattern = "    6   167   167   167   167   167   167     0  1000";

        String actual = (String) records.get(5);
        assertTrue(actual.matches(expectedPattern));
    }

    private int countRecords(String table) {
        Datasource datasource = dbServer.getEmissionsDatasource();
        return tableReader.count(datasource.getName(), table);
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
        return line.startsWith("/");
    }

}
