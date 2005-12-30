package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFTableImporter;

public class NIFNonRoadTableImporter implements Importer {

    private NIFTableImporter delegate;

    public NIFNonRoadTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        delegate = new NIFTableImporter(tables, dataset, new NIFNonRoadTableDatasetTypeUnits(tables, datasource, sqlDataTypes), datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

    public InternalSource[] internalSources() {
        return delegate.internalSources();
    }

}
