package gov.epa.emissions.commons.db.postgres;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;

import java.sql.Connection;
import java.sql.SQLException;

//Note: Emissions & Reference are two schemas in a single database i.e. share a connection
public class PostgresDbServer implements DbServer {

    private SqlDataTypes typeMapper;

    private Datasource emissionsDatasource;

    private Datasource referenceDatasource;

    private Connection connection;

    public PostgresDbServer(Connection connection, String referenceDatasourceName, String emissionsDatasourceName) {
        this.typeMapper = new PostgresSqlDataType();
        this.connection = connection;

        referenceDatasource = createDatasource(referenceDatasourceName, connection);
        emissionsDatasource = createDatasource(emissionsDatasourceName, connection);
    }

    public Datasource getEmissionsDatasource() {
        return emissionsDatasource;
    }

    public Datasource getReferenceDatasource() {
        return referenceDatasource;
    }

    private Datasource createDatasource(String datasourceName, Connection connection) {
        return new PostgresDatasource(datasourceName, connection);
    }

    public SqlDataTypes getDataType() {
        return typeMapper;
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
