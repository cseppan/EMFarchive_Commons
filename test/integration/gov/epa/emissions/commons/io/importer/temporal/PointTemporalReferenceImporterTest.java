package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.OptionalColumnsTableMetadata;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.Random;

public class PointTemporalReferenceImporterTest extends DbTestCase {

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

        PointTemporalReferenceColumnsMetadata base = new PointTemporalReferenceColumnsMetadata(sqlDataTypes);
        OptionalColumnsTableMetadata cols = new OptionalColumnsTableMetadata(base, sqlDataTypes);
        createTable("POINT_SOURCE", datasource, cols);
    }

    protected void tearDown() throws Exception {
        dropTable("POINT_SOURCE", datasource);
    }

    public void testShouldImportAFileWithVariableCols() throws Exception {
        File file = new File("test/data/temporal-crossreference/point-source-VARIABLE-COLS.txt");

        PointTemporalReferenceImporter importer = new PointTemporalReferenceImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        assertEquals(20, countRecords("POINT_SOURCE"));
    }

    private int countRecords(String table) {
        TableReader tableReader = new TableReader(datasource.getConnection());
        return tableReader.count(datasource.getName(), table);
    }

    public void testShouldSetFullLineCommentsAndDescCommentsAsDatasetDescriptionOnImport() throws Exception {
        File file = new File("test/data/temporal-crossreference/point-source.txt");

        PointTemporalReferenceImporter importer = new PointTemporalReferenceImporter(datasource, sqlDataTypes);
        importer.run(file, dataset);

        // assert
        String expected = "# comment1\n#comment 2  \n#comment 3\n";
        assertEquals(expected, dataset.getDescription());
    }

}
