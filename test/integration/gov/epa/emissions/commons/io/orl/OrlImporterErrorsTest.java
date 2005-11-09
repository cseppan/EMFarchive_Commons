package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.framework.db.TableReader;

import java.io.File;
import java.util.Random;

public class OrlImporterErrorsTest extends DbTestCase {

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

    public void testShouldDropTableOnEncounteringMissingTokensInData() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "BAD-point.txt");
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error(missing cols) on record 5, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error(missing cols) on record 5, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingORLTagInHeader() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "MISSING-ORL-TAG-IN-HEADER-point.txt");
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error due to missing 'ORL' tag, and dropped the table", tableReader
                    .exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'ORL' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingCountryTagInHeader() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "MISSING-COUNTRY-TAG-IN-HEADER-point.txt");            
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error due to missing 'COUNTRY' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'COUNTRY' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringEmptyCountryTagInHeader() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "MISSING-COUNTRY-TAG-IN-HEADER-point.txt");            
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error due to empty 'COUNTRY' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to empty 'COUNTRY' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingYearTagInHeader() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "MISSING-YEAR-TAG-IN-HEADER-point.txt");            
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error due to missing 'YEAR' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'YEAR' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringEmptyYearTagInHeader() throws Exception {
        ORLPointImporter importer = new ORLPointImporter(datasource, sqlDataTypes, dataset);

        try {
            importer.preCondition(new File("test/data/orl/nc"), "MISSING-YEAR-TAG-IN-HEADER-point.txt");            
            importer.run(dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error due to empty 'YEAR' tag, and dropped the table", tableReader
                    .exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to empty 'YEAR' tag, and dropped the table");
    }

}
