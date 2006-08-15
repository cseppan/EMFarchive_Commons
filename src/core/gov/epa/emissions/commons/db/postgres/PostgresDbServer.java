package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;

import java.sql.Connection;
import java.sql.SQLException;

//Note: Emissions & Reference are two schemas in a single database i.e. share a connection
public class PostgresDbServer implements DbServer {

    private SqlDataTypes types;

    private Datasource emissionsDatasource;

    private Datasource referenceDatasource;

    private Datasource emfDatasource;

    private Connection connection;

    public PostgresDbServer(Connection connection, String referenceDatasourceName, String emissionsDatasourceName,
            String emfDatasourceName) {
        this.types = new PostgresSqlDataTypes();
        this.connection = connection;

        referenceDatasource = createDatasource(referenceDatasourceName, connection);
        emissionsDatasource = createDatasource(emissionsDatasourceName, connection);
        emfDatasource = createDatasource(emfDatasourceName, connection);
    }

    public Datasource getEmissionsDatasource() {
        return emissionsDatasource;
    }

    public Datasource getReferenceDatasource() {
        return referenceDatasource;
    }

    public Datasource getEmfDatasource() {
        return emfDatasource;
    }

    private Datasource createDatasource(String datasourceName, Connection connection) {
        return new PostgresDatasource(datasourceName, connection, types);
    }

    public SqlDataTypes getSqlDataTypes() {
        return types;
    }

    public String asciiToNumber(String asciiColumn, int precision) {
        StringBuffer precisionBuf = new StringBuffer();
        for (int i = 0; i < precision; i++) {
            precisionBuf.append('9');
        }

        return "to_number(" + asciiColumn + ", '" + precisionBuf.toString() + "')";
    }

    public void disconnect() throws SQLException {
        connection.close();
    }

}
