package gov.epa.emissions.commons.io.reference;

import gov.epa.emissions.commons.db.DatabaseSetup;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ReferenceDatasourceSetup {

    private ReferenceDatasourceTablesCreator creator;

    public ReferenceDatasourceSetup(String configFile, File base) throws Exception {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File(configFile)));

        DatabaseSetup dbSetup = new DatabaseSetup(properties);
        creator = new ReferenceDatasourceTablesCreator(dbSetup.getDbServer(), base);
    }

    private void run() throws Exception {
        creator.create();
    }

    public static void main(String[] args) throws Exception {
        ReferenceDatasourceSetup setup = new ReferenceDatasourceSetup(args[0], new File(args[1]));
        setup.run();
    }
}
