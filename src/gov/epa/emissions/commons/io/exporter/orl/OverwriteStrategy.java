package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;

import java.io.File;

/**
 * overwrites file, if exists
 */
public class OverwriteStrategy implements WriteStrategy {

    private ORLWriter writer;

    public OverwriteStrategy(DbServer dbServer) {
        writer = new ORLWriter(dbServer);
    }

    public void write(Dataset dataset, File file) throws Exception {
        writer.write(dataset, file);
    }

}