package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;

public class NIFNonPointImporter {

    private NIFImporter delegate;

    private NIFDatasetTypeUnits units;

    public NIFNonPointImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new NIFImporter(datasource);
        units = new NIFDatasetTypeUnits(sqlDataTypes);
    }

    private void preImport(Dataset dataset) throws ImporterException {
        units.processFiles(dataset.getInternalSources());
    }

    public void run(Dataset dataset) throws ImporterException {
        preImport(dataset);
        doImport(dataset);
    }

    private void doImport(Dataset dataset) throws ImporterException {
        DatasetTypeUnit ceTypeUnit = (DatasetTypeUnit) units.controlEfficiencyUnit();
        doImport(dataset, ceTypeUnit);

        DatasetTypeUnit emTypeUnit = (DatasetTypeUnit) units.emissionsUnit();
        doImport(dataset, emTypeUnit);

        DatasetTypeUnit peTypeUnit = (DatasetTypeUnit) units.periodsUnit();
        doImport(dataset, peTypeUnit);

        DatasetTypeUnit epTypeUnit = (DatasetTypeUnit) units.emissionsProcessUnit();
        doImport(dataset, epTypeUnit);
    }

    private void doImport(Dataset dataset, DatasetTypeUnit unit) throws ImporterException {
        InternalSource internalSource = unit.getInternalSource();
        if(internalSource==null){
            return;
        }
        
        delegate.run(internalSource, unit, dataset);
    }

}
