package gov.epa.emissions.commons.db;

public interface DbServer {

    Datasource getEmissionsDatasource();

    Datasource getReferenceDatasource();

    SqlDataType getTypeMapper();

    /**
     * @return wraps a db-specific function around ascii column to convert it to
     *         a number w/ specified precision (i.e. size)
     */
    String asciiToNumber(String asciiColumn, int precision);

}
