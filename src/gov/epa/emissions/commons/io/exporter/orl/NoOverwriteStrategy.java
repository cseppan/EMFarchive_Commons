package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.EmfDataset;

import java.io.File;

public class NoOverwriteStrategy implements WriteStrategy {

    private ORLWriter writer;

    public NoOverwriteStrategy(DbServer dbServer) {
        writer = new ORLWriter(dbServer);
    }

    // FIXME: use ImportExport Exception instead
    public void write(EmfDataset dataset, File file) throws Exception {
        if (file.exists())
            throw new Exception("Cannot export as file - " + file.getAbsolutePath() + " exists");

        writer.write(dataset, file);
    }

}
