package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.PersistenceTestCase;

import java.io.File;
import java.util.Random;

public class OrlImporterErrorsTest extends PersistenceTestCase {

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

    public void testShouldDropTableOnEncounteringMissingTokensInData() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "BAD-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error(missing cols) on record 5, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error(missing cols) on record 5, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingORLTagInHeader() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "MISSING-ORL-TAG-IN-HEADER-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error due to missing 'ORL' tag, and dropped the table", tableReader
                    .exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'ORL' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingCountryTagInHeader() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "MISSING-COUNTRY-TAG-IN-HEADER-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error due to missing 'COUNTRY' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'COUNTRY' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringEmptyCountryTagInHeader() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "MISSING-COUNTRY-TAG-IN-HEADER-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error due to empty 'COUNTRY' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to empty 'COUNTRY' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringMissingYearTagInHeader() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "MISSING-YEAR-TAG-IN-HEADER-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);

            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error due to missing 'YEAR' tag, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to missing 'YEAR' tag, and dropped the table");
    }

    public void testShouldDropTableOnEncounteringEmptyYearTagInHeader() throws Exception {
        try {
            File file = new File("test/data/orl/nc", "MISSING-YEAR-TAG-IN-HEADER-point.txt");
            ORLPointImporter importer = new ORLPointImporter(file, dataset, datasource, sqlDataTypes);
            importer.run();
        } catch (ImporterException e) {
            TableReader tableReader = tableReader(datasource);
            assertFalse("should have encountered an error due to empty 'YEAR' tag, and dropped the table", tableReader
                    .exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error due to empty 'YEAR' tag, and dropped the table");
    }

}
