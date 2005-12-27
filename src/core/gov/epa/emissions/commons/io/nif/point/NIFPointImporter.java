package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

import java.io.File;

public class NIFPointImporter implements Importer {

    private NIFImporter delegate;

    public NIFPointImporter(File[] files, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        String tablePrefix = new DataTable(dataset, datasource).name();
        delegate = new NIFImporter(files, dataset, new NIFPointDatasetTypeUnits(files, tablePrefix, sqlDataTypes),
                datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
