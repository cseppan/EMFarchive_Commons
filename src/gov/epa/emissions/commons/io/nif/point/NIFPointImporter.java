package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFPointImporter {

    private NIFImporter delegate;

    public NIFPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(datasource,new NIFPointDatasetTypeUnits(sqlDataTypes));
    }

    public void preImport(Dataset dataset) throws ImporterException {
        delegate.preImport(dataset);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run(dataset);
    }

}
