package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFNonPointImporter {

    private NIFImporter delegate;

    public NIFNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(datasource, new NIFNonPointDatasetTypeUnits(sqlDataTypes));
    }

    public void preImport(Dataset dataset) throws ImporterException {
        delegate.preImport(dataset);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run(dataset);
    }

}
