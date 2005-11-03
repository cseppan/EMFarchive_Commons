package gov.epa.emissions.commons.io.nif.point;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFPointImporter {

    private NIFImporter delegate;

    private NIFPointDatasetTypeUnits units;

    public NIFPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(datasource);
        units = new NIFPointDatasetTypeUnits(sqlDataTypes);
    }

    private void preImport(Dataset dataset) throws ImporterException {
        units.processFiles(dataset.getInternalSources());
    }

    public void run(Dataset dataset) throws ImporterException {
        preImport(dataset);
        doImport(dataset);
    }

    private void doImport(Dataset dataset) throws ImporterException {
        FormatUnit[] typeUnits = units.formatUnits();
        for (int i = 0; i < typeUnits.length; i++) {
            doImport(dataset, typeUnits[i]);
        }
    }

    private void doImport(Dataset dataset, FormatUnit unit) throws ImporterException {
        InternalSource internalSource = unit.getInternalSource();
        if (internalSource == null) {
            return;
        }
        delegate.run(internalSource, unit, dataset);
    }
}
