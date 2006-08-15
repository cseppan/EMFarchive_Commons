package gov.epa.emissions.commons.db;

import java.sql.SQLException;

public interface DbServer {

    Datasource getEmissionsDatasource();

    Datasource getReferenceDatasource();
    
    Datasource getEmfDatasource();

    SqlDataTypes getSqlDataTypes();

    /**
     * @return wraps a db-specific function around ascii column to convert it to
     *         a number w/ specified precision (i.e. size)
     */
    String asciiToNumber(String asciiColumn, int precision);

    void disconnect() throws SQLException;
}
