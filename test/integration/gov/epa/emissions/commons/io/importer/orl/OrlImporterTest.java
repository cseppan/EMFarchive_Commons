package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.InternalSource;
import gov.epa.emissions.commons.io.importer.temporal.TableColumnsMetadata;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

public class OrlImporterTest extends DbTestCase {

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

    public void testShouldImportASmallAndSimplePointFile() throws Exception {
        File file = new File("test/data/orl/nc/small-point.txt");

        OrlPointImporter importer = new OrlPointImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        assertEquals(10, countRecords());
    }

    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallAndSimpleNonPointFile() throws Exception {
        File file = new File("test/data/orl/nc/small-nonpoint.txt");

        OrlNonPointImporter importer = new OrlNonPointImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        assertEquals(6, countRecords());
        //dataset properties
        List sources = dataset.getInternalSources();
        assertEquals(1, sources.size());
        InternalSource ds = (InternalSource) sources.get(0);
        assertEquals(dataset.getName(), ds.getTable());
        assertEquals("ORL NonPoint", ds.getType());
        ColumnsMetadata cols = new TableColumnsMetadata(new OrlNonPointColumnsMetadata(sqlDataTypes), sqlDataTypes);
        assertEquals(cols.colNames().length, ds.getCols().length);
        assertEquals(file.getAbsolutePath(), ds.getSource());
        // TODO: size ?
    }

    public void testShouldImportASmallAndSimpleNonRoadFile() throws Exception {
        File file = new File("test/data/orl/nc/small-nonroad.txt");

        OrlNonRoadImporter importer = new OrlNonRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        assertEquals(16, countRecords());
    }

    public void testShouldImportASmallAndSimpleOnRoadFile() throws Exception {
        File file = new File("test/data/orl/nc/small-onroad.txt");

        OrlOnRoadImporter importer = new OrlOnRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        assertEquals(18, countRecords());
    }

    public void testShouldLoadCountryRegionYearIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc/small-onroad.txt");

        OrlOnRoadImporter importer = new OrlOnRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        assertEquals("PERU", dataset.getCountry());
        assertEquals("PERU", dataset.getRegion());
        assertEquals(1995, dataset.getYear());
    }

    public void testShouldLoadStartStopDateTimeIntoDatasetOnImport() throws Exception {
        File file = new File("test/data/orl/nc/small-onroad.txt");

        OrlOnRoadImporter importer = new OrlOnRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        Date start = dataset.getStartDateTime();
        Date expectedStart = new GregorianCalendar(1995, Calendar.JANUARY, 1).getTime();
        assertEquals(expectedStart, start);

        Date end = dataset.getStopDateTime();
        GregorianCalendar endCal = new GregorianCalendar(1995, Calendar.DECEMBER, 31, 23, 59, 59);
        endCal.set(Calendar.MILLISECOND, 999);
        assertEquals(endCal.getTime(), end);
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        File file = new File("test/data/orl/nc/small-onroad.txt");

        OrlOnRoadImporter importer = new OrlOnRoadImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        String expected = " Created from file 99OR-MOD.TXT provided by M. Strum in November 2002.\n"
                + " North Carolina data extracted from original file using UNIX grep command.\n"
                + "    paste commands. \n";
        assertEquals(expected, dataset.getDescription());
    }

}
