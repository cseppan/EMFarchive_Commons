package gov.epa.emissions.commons.db.mysql;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.legacy.ReferenceImporter;
import gov.epa.emissions.commons.io.ref.ReferenceTablesCreator;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Note: Emissions & Reference are two schemas in a single database i.e. share a
 * connection. A datasource is represented by a schema in MySql, and Database ==
 * Schema
 */
public class MySqlDbServer implements DbServer {

    private SqlDataTypes types;

    private Datasource emissionsDatasource;

    private Datasource referenceDatasource;

    private Connection connection;

    public MySqlDbServer(Connection connection, String referenceDatasourceName, String emissionsDatasourceName,
            File fieldDefsFile, File referenceFilesDir) throws SQLException {
        this.types = new MySqlDataTypes();
        this.connection = connection;

        createEmissionsDatasource(connection, emissionsDatasourceName);
        createReferenceDatasource(connection, referenceDatasourceName, fieldDefsFile, referenceFilesDir);
    }

    private void createEmissionsDatasource(Connection connection, String datasourceName) throws SQLException {
        emissionsDatasource = new MySqlDatasource(datasourceName, connection, types);

        if (!doesSchemaExist(datasourceName, emissionsDatasource.getConnection()))
            createSchema(datasourceName, emissionsDatasource.getConnection());
    }

    private void createReferenceDatasource(Connection connection, String datasourceName, File fieldDefsFile,
            File referenceFilesDir) throws SQLException {
        referenceDatasource = new MySqlDatasource(datasourceName, connection, types);

        if (!doesSchemaExist(datasourceName, referenceDatasource.getConnection())) {
            createSchema(datasourceName, referenceDatasource.getConnection());
            createReferenceTables(fieldDefsFile, referenceFilesDir);
        }
    }

    private void createReferenceTables(File fieldDefsFile, File referenceFilesDir) throws SQLException {
        try {
            ReferenceImporter importer = new ReferenceImporter(this, fieldDefsFile, referenceFilesDir, false, null);
            importer.run();

            ReferenceTablesCreator tables = new ReferenceTablesCreator(null, getSqlDataTypes());
            tables.createAdditionalRefTables(referenceDatasource);
        } catch (Exception e) {
            throw new SQLException("could not create reference tables. Reason: " + e.getMessage());
        }
    }

    public Datasource getEmissionsDatasource() {
        return emissionsDatasource;
    }

    public Datasource getReferenceDatasource() {
        return referenceDatasource;
    }

    private void createSchema(String datasourceName, Connection connection) throws SQLException {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.execute("CREATE DATABASE " + datasourceName);
        } finally {
            if (stmt != null)
                stmt.close();
        }
    }

    private boolean doesSchemaExist(String datasourceName, Connection connection) throws SQLException {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW databases");
            while (rs.next()) {
                String aDatasourceName = rs.getString(1);
                if (aDatasourceName.equalsIgnoreCase(datasourceName))
                    return true;
            }
        } finally {
            if (stmt != null)
                stmt.close();
        }

        return false;
    }

    public SqlDataTypes getSqlDataTypes() {
        return types;
    }

    public String asciiToNumber(String asciiColumn, int precision) {
        return asciiColumn;
    }

    public void disconnect() throws SQLException {
        connection.close();
    }

}
