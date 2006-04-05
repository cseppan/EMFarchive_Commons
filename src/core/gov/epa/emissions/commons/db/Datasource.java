package gov.epa.emissions.commons.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface Datasource {

    DataQuery query();
    
    OptimizedQuery optimizedQuery(String query) throws SQLException;

    TableDefinition tableDefinition();

    String getName();

    Connection getConnection();

    DataModifier dataModifier();
}
