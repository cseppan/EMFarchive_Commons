package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DatabaseSetup;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.Properties;

import junit.framework.TestCase;

public abstract class PersistenceTestCase extends TestCase {

    protected DatabaseSetup dbSetup;

    protected File fieldDefsFile;

    protected File referenceFilesDir;

    protected void setUp() throws Exception {
        String folder = "test";
        File conf = new File(folder, "test.conf");

        if (!conf.exists() || !conf.isFile()) {
            String error = "File: " + conf + " does not exist. Please copy either of the two TEMPLATE files "
                    + "(from " + folder + "), name it test.conf, configure " + "it as needed, and rerun.";
            throw new RuntimeException(error);
        }

        Properties properties = new Properties();
        properties.load(new FileInputStream(conf));

        properties.put("DATASET_NIF_FIELD_DEFS", "config/field_defs.dat");
        properties.put("REFERENCE_FILE_BASE_DIR", "config/refDbFiles");

        dbSetup = new DatabaseSetup(properties);
        fieldDefsFile = new File("config/field_defs.dat");
        referenceFilesDir = new File("config/refDbFiles");
    }

    protected void tearDown() throws Exception {
        dbSetup.tearDown();
    }

    protected Datasource emissions() {
        return dbServer().getEmissionsDatasource();
    }

    private DbServer dbServer() {
        return dbSetup.getDbServer();
    }

    protected SqlDataTypes dataTypes() {
        return dbServer().getDataType();
    }

    protected void createTable(String table, Datasource datasource, FileFormat cols) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, cols.cols());
    }

    protected void dropTable(String table, Datasource datasource) throws Exception, SQLException {
        DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
        dbUpdate.dropTable(datasource.getName(), table);
    }
}
