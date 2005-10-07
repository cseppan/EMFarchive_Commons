package gov.epa.emissions.commons.db;

import java.sql.Connection;

public interface Datasource {

    DataQuery query();

    TableDefinition tableDefinition();

    String getName();

    Connection getConnection();

    DataModifier getDataModifier();
}
