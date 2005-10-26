package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TemporalResolution;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;
import gov.epa.emissions.framework.db.DbUpdate;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

import org.dbunit.dataset.ITable;

public class ORLImporterTest extends DbTestCase {

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
        dataset.setDatasetType(orlPointDatasetType());
        
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes);

        importer.preCondition(new File("test/data/orl/nc"), "small-point.txt");
        importer.run(dataset);

        assertEquals(10, countRecords());
    }
    
    private DatasetType orlPointDatasetType() {
        FileFormatWithOptionalCols fileFormat = new ORLPointFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);
        DatasetType datasetType = new DatasetType("ORL Point",new DatasetTypeUnitWithOptionalCols[]{unit});
        return datasetType;
    }

    private int countRecords() {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), dataset.getName());
    }

    public void testShouldImportASmallAndSimpleNonPointFile() throws Exception {
        dataset.setDatasetType(orlNonPointDatasetType());
        
        ORLNonPointImporter importer = new ORLNonPointImporter(datasource);

        importer.preCondition(new File("test/data/orl/nc"), "small-nonpoint.txt");
        importer.run(dataset);

        assertEquals(6, countRecords());
    }

    public void testShouldImportNonPointFileWithVaryingCols() throws Exception {
        dataset.setDatasetType(orlNonPointDatasetType());
        
        ORLNonPointImporter importer = new ORLNonPointImporter(datasource);

        importer.preCondition(new File("test/data/orl/nc"), "varying-cols-nonpoint.txt");
        importer.run(dataset);

        TableReader tableReader = new TableReader(datasource.getConnection());

        assertEquals(6, tableReader.count(datasource.getName(), dataset.getName()));
        ITable table = tableReader.table(datasource.getName(), dataset.getName());
        assertNull(table.getValue(0, "CEFF"));
        assertNull(table.getValue(0, "REFF"));
        assertNull(table.getValue(0, "RPEN"));

        assertNull(table.getValue(1, "CEFF"));
        assertNull(table.getValue(1, "REFF"));
        assertEquals(new Float(1.0), table.getValue(1, "RPEN"));

        assertNull(table.getValue(2, "CEFF"));
        assertNull(table.getValue(2, "REFF"));
        assertNull(table.getValue(2, "RPEN"));
    }

    public void testShouldLoadInternalSourceIntoDatasetOnImport() throws Exception {
        dataset.setDatasetType(orlNonPointDatasetType());
        
        ORLNonPointImporter importer = new ORLNonPointImporter(datasource);
        
        File fullpath = new File(new File("test/data/orl/nc"), "small-nonpoint.txt");
        importer.preCondition(new File("test/data/orl/nc"), "small-nonpoint.txt");
        importer.run(dataset);

        InternalSource[] sources = dataset.getInternalSources();
        assertEquals(1, sources.length);
        InternalSource source = sources[0];
        assertEquals(dataset.getName(), source.getTable());
        assertEquals("ORL NonPoint", source.getType());

        FileFormat colsMetadata = new FixedColsTableFormat(new ORLNonPointFileFormat(sqlDataTypes),
                sqlDataTypes);
        String[] actualCols = source.getCols();
        String[] expectedCols = colNames(colsMetadata.cols());
        assertEquals(expectedCols.length, actualCols.length);
        for (int i = 0; i < actualCols.length; i++) {
            assertEquals(expectedCols[i], actualCols[i]);
        }

        assertEquals(fullpath.getAbsolutePath(), source.getSource());
        assertEquals(fullpath.length(), source.getSourceSize());
    }
    
    private DatasetType orlNonPointDatasetType() {
        ORLNonPointFileFormat fileFormat = new ORLNonPointFileFormat(
                sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);
        DatasetType datasetType = new DatasetType("ORL Nonpoint",new DatasetTypeUnitWithOptionalCols[]{unit});
        return datasetType;
    }

    private String[] colNames(Column[] cols) {
        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++)
            names.add(cols[i].name());

        return (String[]) names.toArray(new String[0]);
    }

    public void testShouldImportASmallAndSimpleNonRoadFile() throws Exception {
        dataset.setDatasetType(orlNonRoadDatasetType());
        
        ORLNonRoadImporter importer = new ORLNonRoadImporter(datasource);
        importer.preCondition(new File("test/data/orl/nc"), "small-nonroad.txt");
        importer.run(dataset);

        assertEquals(16, countRecords());
    }
    
    private DatasetType orlNonRoadDatasetType() {
        ORLNonRoadFileFormat fileFormat = new ORLNonRoadFileFormat(
                sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);
        DatasetType datasetType = new DatasetType("ORL Nonroad",new DatasetTypeUnitWithOptionalCols[]{unit});
        return datasetType;
    }

    public void testShouldImportASmallAndSimpleOnRoadFile() throws Exception {
        dataset.setDatasetType(orlOnRoadDatasetType());
        
        File folder = new File("test/data/orl/nc");
        String filename = "small-onroad.txt";

        Importer importer = new ORLOnRoadImporter(datasource);
        importer.preCondition(folder, filename);
        importer.run(dataset);

        assertEquals(18, countRecords());
    }

    public void testShouldLoadCountryRegionYearIntoDatasetOnImport() throws Exception {
        dataset.setDatasetType(orlOnRoadDatasetType());
        
        Importer importer = new ORLOnRoadImporter(datasource);
        importer.preCondition(new File("test/data/orl/nc"), "small-onroad.txt");
        importer.run(dataset);

        // assert
        assertEquals("PERU", dataset.getCountry());
        assertEquals("PERU", dataset.getRegion());
        assertEquals(1995, dataset.getYear());
    }

    public void testShouldLoadStartStopDateTimeIntoDatasetOnImport() throws Exception {
        dataset.setDatasetType(orlOnRoadDatasetType());
        
        Importer importer = new ORLOnRoadImporter(datasource);
        importer.preCondition(new File("test/data/orl/nc"), "small-onroad.txt");
        importer.run(dataset);

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
        dataset.setDatasetType(orlOnRoadDatasetType());
        
        Importer importer = new ORLOnRoadImporter(datasource);
        importer.preCondition(new File("test/data/orl/nc"), "small-onroad.txt");
        importer.run(dataset);

        // assert
        assertEquals(TemporalResolution.ANNUAL.getName(), dataset.getTemporalResolution());
        assertEquals("short tons/year", dataset.getUnits());
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        dataset.setDatasetType(orlOnRoadDatasetType());
        
        Importer importer = new ORLOnRoadImporter(datasource);
        importer.preCondition(new File("test/data/orl/nc"), "small-onroad.txt");
        importer.run(dataset);

        // assert
        String expected = "#ORL\n#TYPE    Mobile source toxics inventory, on-road mobile source only\n"
                + "#COUNTRY PERU\n#YEAR    1995\n#DESC Created from file 99OR-MOD.TXT provided by M. Strum in "
                + "November 2002.\n"
                + "#DESC North Carolina data extracted from original file using UNIX grep command.\n"
                + "#DESC    paste commands. \n" + "#comment 1\n#comment 2\n#comment 3\n#comment 4\n";
        assertEquals(expected, dataset.getDescription());
    }
    
    private DatasetType orlOnRoadDatasetType() {
        ORLOnRoadFileFormat fileFormat = new ORLOnRoadFileFormat(
                sqlDataTypes);
        TableFormatWithOptionalCols tableColsMetadata = new TableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        DatasetTypeUnitWithOptionalCols unit = new DatasetTypeUnitWithOptionalCols(tableColsMetadata, fileFormat);
        DatasetType datasetType = new DatasetType("ORL Onroad",new DatasetTypeUnitWithOptionalCols[]{unit});
        return datasetType;
    }

}