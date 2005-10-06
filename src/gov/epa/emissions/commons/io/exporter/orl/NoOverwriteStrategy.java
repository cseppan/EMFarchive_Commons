package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;

import java.io.File;

public class NoOverwriteStrategy implements WriteStrategy {

    private ORLWriter writer;

    public NoOverwriteStrategy(DbServer dbServer, ORLDatasetTypesFactory typesFactory) {
        writer = new ORLWriter(dbServer, typesFactory);
    }

    // FIXME: use ImportExport Exception instead
    public void write(Dataset dataset, File file) throws Exception {
        if (file.exists())
            throw new Exception("Cannot export as file - " + file.getAbsolutePath() + " exists");

        writer.write(dataset, file);
    }

}
