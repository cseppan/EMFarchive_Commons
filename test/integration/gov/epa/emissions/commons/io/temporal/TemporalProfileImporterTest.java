package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class TemporalProfileImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes typeMapper;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        typeMapper = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), "Monthly");
        dbUpdate.dropTable(datasource.getName(), "Weekly");
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekday");
        dbUpdate.dropTable(datasource.getName(), "Diurnal_Weekend");
        dbUpdate.deleteAll(datasource.getName(), "versions");
    }

    public void testShouldReadFromFileAndLoadMonthlyPacketIntoTable() throws Exception {
        File file = new File("test/data/temporal-profiles/small.txt");

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));

        TemporalProfileImporter importer = new TemporalProfileImporter(file, dataset, datasource, typeMapper);
        importer.run();

        // assert
        assertEquals(10, countRecords("Monthly"));
        assertVersionInfo("Monthly", countRecords("Monthly"));

        assertEquals(13, countRecords("WEEKLY"));
        assertVersionInfo("WEEKLY", countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, datasource, typeMapper);
        File exportfile = new File("test/data/temporal-profiles", "VersionedTemporalProfileExported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        exportfile.delete();
    }

    private void assertVersionInfo(String name, int rows) throws Exception {
        verifyVersionCols(name, rows);
    }

    private void verifyVersionCols(String table, int rows) throws Exception {
        TableReader tableReader = tableReader(datasource);

        ITable tableRef = tableReader.table(datasource.getName(), table);
        for (int i = 0; i < rows; i++) {
            Object recordId = tableRef.getValue(i, "Record_Id");
            assertEquals((i + 1) + "", recordId.toString());

            Object version = tableRef.getValue(i, "Version");
            assertEquals("0", version.toString());

            Object deleteVersions = tableRef.getValue(i, "Delete_Versions");
            assertEquals("", deleteVersions);
        }
    }

    private int countRecords(String table) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), table);
    }

}
