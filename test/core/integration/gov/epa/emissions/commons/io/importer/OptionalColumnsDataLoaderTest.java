package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.VersionedDataFormatFactory;
import gov.epa.emissions.commons.io.orl.ORLNonPointFileFormat;
import gov.epa.emissions.commons.io.temporal.FixedColsTableFormat;
import gov.epa.emissions.commons.io.temporal.PointTemporalReferenceFileFormat;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

import java.io.File;
import java.sql.SQLException;

import org.dbunit.dataset.ITable;

public class OptionalColumnsDataLoaderTest extends PersistenceTestCase {

    private Datasource datasource;

    private SqlDataTypes sqlDataTypes;

    private String table;

    protected void setUp() throws Exception {
        super.setUp();

        DbServer dbServer = dbSetup.getDbServer();
        sqlDataTypes = dbServer.getSqlDataTypes();
        datasource = dbServer.getEmissionsDatasource();
        table = "varying";
    }

    protected void tearDown() throws Exception {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), table);
    }

    private FileFormatWithOptionalCols setupUnversionedTable() throws SQLException {
        FileFormatWithOptionalCols fileFormat = new PointTemporalReferenceFileFormat(sqlDataTypes);
        FixedColsTableFormat format = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        createTable(table, datasource, format);

        return fileFormat;
    }

    public void testShouldLoadRecordsFromFileWithVariableColsIntoUnversionedTable() throws Exception {
        FileFormatWithOptionalCols fileFormat = setupUnversionedTable();
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, fileFormat, "Dataset_Id");

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        File file = new File("test/data/variable-cols.txt");
        Reader reader = new WhitespaceDelimitedFileReader(file);
        loader.load(reader, dataset, table);

        // assert
        TableReader tableReader = tableReader(datasource);

        assertTrue("Table '" + table + "' should have been created", tableReader.exists(datasource.getName(), table));
        assertEquals(6, tableReader.count(datasource.getName(), table));
    }

    private TableFormat setupVersionedTable(FileFormatWithOptionalCols fileFormat) throws SQLException {
        TableFormat tableFormat = new VersionedTableFormat(fileFormat, sqlDataTypes);
        createTable(table, datasource, tableFormat);

        return tableFormat;
    }

    public void testShouldLoadRecordsFromFileIntoVersionedTable() throws Exception {
        // create table
        VersionedDataFormatFactory formatFactory = new VersionedDataFormatFactory();
        ORLNonPointFileFormat fileFormat = new ORLNonPointFileFormat(sqlDataTypes, formatFactory.defaultValuesFiller());
        TableFormat tableFormat = setupVersionedTable(fileFormat);

        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, fileFormat, tableFormat.key());

        Dataset dataset = new SimpleDataset();
        dataset.setName("test");

        File file = new File("test/data/orl/nc", "small-nonpoint.txt");
        Reader reader = new DelimiterIdentifyingFileReader(file, fileFormat.minCols().length);

        loader.load(reader, dataset, table);

        // assert
        TableReader tableReader = tableReader(datasource);

        assertTrue("Table '" + table + "' should have been created", tableReader.exists(datasource.getName(), table));
        int rows = tableReader.count(datasource.getName(), table);
        assertEquals(6, rows);

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

}
