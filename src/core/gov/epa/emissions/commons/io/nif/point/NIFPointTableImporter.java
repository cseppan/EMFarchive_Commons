package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFTableImporter;

public class NIFPointTableImporter implements Importer {

    private NIFTableImporter delegate;

    public NIFPointTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        delegate = new NIFTableImporter(tables, dataset, new NIFPointTableDatasetTypeUnits(tables, datasource, sqlDataTypes), datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
