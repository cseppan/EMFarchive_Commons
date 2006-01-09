package gov.epa.emissions.commons.db;

import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;

public interface DbUpdate {

    public abstract void deleteAll(String schema, String table) throws DatabaseUnitException, SQLException;

    // DELETE from table where name=value
    public abstract void delete(String table, String name, String value) throws SQLException, DatabaseUnitException;

    public abstract void delete(String table, String name, int value) throws SQLException, DatabaseUnitException;

    public abstract void dropTable(String schema, String table) throws SQLException;

}