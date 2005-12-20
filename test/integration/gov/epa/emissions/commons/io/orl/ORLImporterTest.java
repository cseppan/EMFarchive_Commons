package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.db.version.Versions;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.TemporalResolution;
import gov.epa.emissions.commons.io.importer.VersionedTableFormatWithOptionalCols;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class ORLImporterTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private Dataset dataset;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();

        dataset = new SimpleDataset();
        dataset.setName("test");
        dataset.setDatasetid(Math.abs(new Random().nextInt()));
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), dataset.getName());

        dbUpdate.deleteAll(datasource.getName(), "versions");
    }

    public void testShouldImportASmallAndSimplePointFile() throws Exception {
        File file = new File("test/data/orl/nc", "small-point.txt");
        ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(10, rows);

        assertVersionInfo(dataset.getName(), rows);
    }

    private void assertVersionInfo(String name, int rows) throws Exception {
        verifyVersionCols(name, rows);
        verifyVersionZeroEntryInVersionsTable();
    }

    // FIXME: the parser is not working properly
    public void FIXME_testShouldImportASmallAndSimpleExtendedPointFile() throws Exception {
        File file = new File("test/data/orl/extended", "orl-extended-point.txt");
        ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        assertEquals(200, countRecords());
    }

    public void testShouldImportASmallAndSimpleNonPointFile() throws Exception {
        File file = new File("test/data/orl/nc", "small-nonpoint.txt");
        ORLNonPointImporter importer = new ORLNonPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        
        assertEquals(6, countRecords());

        // assert
        TableReader tableReader = tableReader(datasource);

        String table = dataset.getName();
        assertTrue("Table '" + table + "' should have been created", tableReader.exists(datasource.getName(), table));

        int rows = tableReader.count(datasource.getName(), table);
        assertEquals(6, rows);
        assertVersionInfo(table, rows);
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

    private void verifyVersionZeroEntryInVersionsTable() throws Exception {
        Versions versions = new Versions(datasource);
        Version[] onRoadVersions = versions.get(dataset.getDatasetid());
        assertEquals(1, onRoadVersions.length);

        Version versionZero = onRoadVersions[0];
        assertEquals(0, versionZero.getVersion());
        assertEquals(dataset.getDatasetid(), versionZero.getDatasetId());
        assertEquals("", versionZero.getPath());
        assertTrue("Version Zero should be zero upon import", versionZero.isFinalVersion());
    }

    // FIXME: the parser is not working properly
    public void FIXME_testShouldImportASmallAndSimpleExtendedNonPointFile() throws Exception {
        File file = new File("test/data/orl/extended", "orl-extended-nonpoint.txt");
        ORLNonPointImporter importer = new ORLNonPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();
        assertEquals(200, countRecords());
    }

    public void testShouldImportNonPointFileWithVaryingCols() throws Exception {
        File file = new File("test/data/orl/nc", "varying-cols-nonpoint.txt");
        ORLNonPointImporter importer = new ORLNonPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        TableReader tableReader = tableReader(datasource);

        int rows = tableReader.count(datasource.getName(), dataset.getName());
        assertEquals(6, rows);
        ITable table = tableReader.table(datasource.getName(), dataset.getName());
        assertNull(table.getValue(0, "CEFF"));
        assertNull(table.getValue(0, "REFF"));
        assertNull(table.getValue(0, "RPEN"));

        assertNull(table.getValue(1, "CEFF"));
        assertNull(table.getValue(1, "REFF"));
        assertEquals(1.0, Double.valueOf(""+table.getValue(1, "RPEN")).doubleValue(),0.000001);

        assertNull(table.getValue(2, "CEFF"));
        assertNull(table.getValue(2, "REFF"));
        assertNull(table.getValue(2, "RPEN"));

        assertVersionInfo(dataset.getName(), rows);
    }

    public void testShouldLoadInternalSourceIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc", "small-nonpoint.txt");
        ORLNonPointImporter importer = new ORLNonPointImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        InternalSource[] sources = dataset.getInternalSources();
        assertEquals(1, sources.length);
        InternalSource source = sources[0];
        assertEquals(dataset.getName(), source.getTable());
        assertEquals("ORL NonPoint", source.getType());

        FileFormat fileFormat = new VersionedTableFormatWithOptionalCols(new ORLNonPointFileFormat(sqlDataTypes),
                sqlDataTypes);
        String[] actualCols = source.getCols();
        String[] expectedCols = colNames(fileFormat.cols());
        assertEquals(expectedCols.length, actualCols.length);
        for (int i = 0; i < actualCols.length; i++) {
            assertEquals(expectedCols[i], actualCols[i]);
        }

        assertEquals(file.getAbsolutePath(), source.getSource());
        assertEquals(file.length(), source.getSourceSize());
    }

    private String[] colNames(Column[] cols) {
        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++)
            names.add(cols[i].name());

        return (String[]) names.toArray(new String[0]);
    }

    public void testShouldImportASmallAndSimpleNonRoadFile() throws Exception {
        File file = new File("test/data/orl/nc", "small-nonroad.txt");
        ORLNonRoadImporter importer = new ORLNonRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(16, rows);
        assertVersionInfo(dataset.getName(), rows);
    }

    // FIXME: the parser is not working properly
    public void FIXME_testShouldImportASmallAndSimpleExtendedNonRoadFile() throws Exception {
        File file = new File("test/data/orl/extended", "orl-extended-nonroad.txt");
        ORLNonRoadImporter importer = new ORLNonRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        assertEquals(200, countRecords());
    }

    public void testShouldImportASmallAndSimpleOnRoadFile() throws Exception {
        File importFile = new File("test/data/orl/nc", "small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(importFile, dataset, datasource, sqlDataTypes);
        importer.run();

        int rows = countRecords();
        assertEquals(18, rows);
        assertVersionInfo(dataset.getName(), rows);
    }

    public void testShouldLoadCountryRegionYearIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc", "small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        // assert
        assertEquals("PERU", dataset.getCountry());
        assertEquals("PERU", dataset.getRegion());
        assertEquals(1995, dataset.getYear());
    }

    public void testShouldLoadStartStopDateTimeIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc", "small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        // assert
        Date start = dataset.getStartDateTime();
        Date expectedStart = new GregorianCalendar(1995, Calendar.JANUARY, 1).getTime();
        assertEquals(expectedStart, start);

        Date end = dataset.getStopDateTime();
        GregorianCalendar endCal = new GregorianCalendar(1995, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        assertEquals(endCal.getTime(), end);
    }

    public void testShouldLoadTemporalResolutionAndUnitsIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc", "small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        // assert
        assertEquals(TemporalResolution.ANNUAL.getName(), dataset.getTemporalResolution());
        assertEquals("short tons/year", dataset.getUnits());
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        File file = new File("test/data/orl/nc", "small-onroad.txt");
        Importer importer = new ORLOnRoadImporter(file, dataset, datasource, sqlDataTypes);
        importer.run();

        // assert
        String expected = "#ORL\n#TYPE    Mobile source toxics inventory, on-road mobile source only\n"
                + "#COUNTRY PERU\n#YEAR    1995\n#DESC Created from file 99OR-MOD.TXT provided by M. Strum in "
                + "November 2002.\n"
                + "#DESC North Carolina data extracted from original file using UNIX grep command.\n"
                + "#DESC    paste commands. \n" + "#comment 1\n#comment 2\n#comment 3\n#comment 4\n";
        assertEquals(expected, dataset.getDescription());
    }

    private int countRecords() {
        TableReader tableReader = tableReader(datasource);
        return tableReader.count(datasource.getName(), dataset.getName());
    }

}
