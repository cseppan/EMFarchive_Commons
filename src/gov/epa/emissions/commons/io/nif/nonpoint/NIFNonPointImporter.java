package gov.epa.emissions.commons.io.nif.nonpoint;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFImporter;

public class NIFNonPointImporter {

    private NIFImporter delegate;

    private NIFNonPointDatasetTypeUnits units;

    public NIFNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(datasource);
        units = new NIFNonPointDatasetTypeUnits(sqlDataTypes);
    }

    private void preImport(Dataset dataset) throws ImporterException {
        units.processFiles(dataset.getInternalSources());
    }

    public void run(Dataset dataset) throws ImporterException {
        preImport(dataset);
        doImport(dataset);
    }

    private void doImport(Dataset dataset) throws ImporterException {
        FormatUnit ceTypeUnit = units.controlEfficiencyUnit();
        doImport(dataset, ceTypeUnit);

        FormatUnit emTypeUnit = units.emissionsUnit();
        doImport(dataset, emTypeUnit);

        FormatUnit peTypeUnit = units.periodsUnit();
        doImport(dataset, peTypeUnit);

        FormatUnit epTypeUnit = units.emissionsProcessUnit();
        doImport(dataset, epTypeUnit);
    }

    private void doImport(Dataset dataset, FormatUnit unit) throws ImporterException {
        InternalSource internalSource = unit.getInternalSource();
        if (internalSource == null) {
            return;
        }
        delegate.run(internalSource, unit, dataset);
    }
}
