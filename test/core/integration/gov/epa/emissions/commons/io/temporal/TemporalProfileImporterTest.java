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
        assertEquals(13, countRecords("WEEKLY"));

        TemporalProfileExporter exporter = new TemporalProfileExporter(dataset, datasource, typeMapper);
        File exportfile = new File("test/data/temporal-profiles", "VersionedTemporalProfileExported.txt");
        exporter.export(exportfile);
        // FIXME: compare the original file and the exported file.
        exportfile.delete();
    }

    private int countRecords(String table) {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), table);
    }

}
