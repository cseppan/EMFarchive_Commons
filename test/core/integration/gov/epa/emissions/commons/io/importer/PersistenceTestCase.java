package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.DatabaseSetup;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.DbUpdate;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.db.TableReader;
import gov.epa.emissions.commons.io.TableFormat;

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
        File conf = new File(folder, configFilename());

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

    private String configFilename() {
        String db = System.getProperty("Database");
        if (db != null && db.equalsIgnoreCase("MYSQL"))
            return "mysql.conf";

        return "postgres.conf";
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
        return dbServer().getSqlDataTypes();
    }

    protected void createTable(String table, Datasource datasource, TableFormat format) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, format.cols());
    }

    protected void dropTable(String table, Datasource datasource) throws Exception, SQLException {
        DbUpdate dbUpdate = dbSetup.dbUpdate(datasource);
        dbUpdate.dropTable(datasource.getName(), table);
    }

    protected void dropData(String table, Datasource datasource) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        modifier.dropAll(table);
    }

    protected TableReader tableReader(Datasource datasource) {
        return dbSetup.tableReader(datasource);
    }
}
