package gov.epa.emissions.framework.db;

import gov.epa.emissions.commons.Config;

import org.apache.commons.configuration.ConfigurationException;

public class PostgresDbConfig extends Config {

    public PostgresDbConfig(String file) throws ConfigurationException {
        super(file);
    }

    public String driver() {
        return "org.postgresql.Driver";
    }

    public String url() {
        return "jdbc:postgresql://" + value("database.host") + "/" + value("database.name");
    }

    public String username() {
        return value("database.username");
    }

    public String password() {
        return value("database.password");
    }
}
