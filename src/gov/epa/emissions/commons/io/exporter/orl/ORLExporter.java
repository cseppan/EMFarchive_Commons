package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.exporter.FixedFormatExporter;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;

import java.io.File;

/**
 * This exporter writes out data in the "One Pollutant Record Per Line" format.
 * It handles four dataset types: Nonpoint, Nonroad, Onroad, and Point. <p/>
 * 
 * By default, it overwrites an existing file.
 */
public class ORLExporter extends FixedFormatExporter {

    private WriteStrategy writeStrategy;

    private ORLExporter(DbServer dbServer, WriteStrategy strategy) {
        super(dbServer);
        this.writeStrategy = strategy;
    }

    public static ORLExporter create(DbServer dbServer, ORLDatasetTypesFactory typesFactory) {
        return new ORLExporter(dbServer, new OverwriteStrategy(dbServer, typesFactory));
    }

    public static ORLExporter createWithoutOverwrite(DbServer dbServer, ORLDatasetTypesFactory typesFactory) {
        return new ORLExporter(dbServer, new NoOverwriteStrategy(dbServer, typesFactory));
    }

    public void run(Dataset dataset, File file) throws Exception {
        writeStrategy.write(dataset, file);
    }
}
