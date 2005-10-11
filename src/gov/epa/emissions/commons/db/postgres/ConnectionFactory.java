package gov.epa.emissions.commons.db.postgres;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionFactory {

    public abstract Connection getConnection() throws SQLException;

}