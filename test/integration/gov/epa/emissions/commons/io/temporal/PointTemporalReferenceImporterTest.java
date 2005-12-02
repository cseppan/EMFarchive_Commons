package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;
import gov.epa.emissions.commons.io.importer.SimpleTableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

import java.io.File;
import java.util.Random;

public class PointTemporalReferenceImporterTest extends PersistenceTestCase {

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

        PointTemporalReferenceFileFormat base = new PointTemporalReferenceFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat = new SimpleTableFormatWithOptionalCols(base, sqlDataTypes);
        createTable("POINT_SOURCE", datasource, tableFormat);
    }

    protected void tearDown() throws Exception {
        dropTable("POINT_SOURCE", datasource);
    }

    public void testShouldImportAFileWithVariableCols() throws Exception {
        File folder = new File("test/data/temporal-crossreference");
        String file = "point-source-VARIABLE-COLS.txt";

        PointTemporalReferenceImporter importer = new PointTemporalReferenceImporter(datasource, sqlDataTypes);
        importer.preCondition(folder, file);
        importer.run(dataset);

        assertEquals(20, countRecords("POINT_SOURCE"));
    }

    private int countRecords(String table) {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), table);
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        PointTemporalReferenceImporter importer = new PointTemporalReferenceImporter(datasource, sqlDataTypes);

        importer.preCondition(new File("test/data/temporal-crossreference"), "point-source.txt");
        importer.run(dataset);

        // assert
        String expected = "# comment1\n#comment 2  \n#comment 3\n";
        assertEquals(expected, dataset.getDescription());
    }

}
