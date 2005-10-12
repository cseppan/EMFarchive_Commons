package gov.epa.emissions.commons.io.importer.orl;

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

    public void testShouldDropTableOnEncounteringBadData() throws Exception {
        File file = new File("test/data/orl/nc/BAD-point.txt");

        OrlPointImporter importer = new OrlPointImporter(datasource, sqlDataTypes);

        try {
            importer.run(file, dataset);
        } catch (ImporterException e) {
            TableReader tableReader = new TableReader(datasource.getConnection());
            assertFalse("should have encountered an error(missing cols) on record 5, and dropped the table",
                    tableReader.exists(datasource.getName(), dataset.getName()));
            return;
        }

        fail("should have encountered an error(missing cols) on record 5, and dropped the table");
    }

}
