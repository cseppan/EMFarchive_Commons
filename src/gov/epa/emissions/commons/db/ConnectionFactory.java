package gov.epa.emissions.commons.db;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionFactory {

    public abstract Connection getConnection() throws SQLException;

}