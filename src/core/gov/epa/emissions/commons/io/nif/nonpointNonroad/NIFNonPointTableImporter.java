package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.nif.NIFTableImporter;

public class NIFNonPointTableImporter implements Importer {

    private NIFTableImporter delegate;

    public NIFNonPointTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        this(tables, dataset, datasource, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public NIFNonPointTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory factory) throws ImporterException {
        NIFNonPointTableDatasetTypeUnits units = new NIFNonPointTableDatasetTypeUnits(tables, datasource, sqlDataTypes,
                factory);
        delegate = new NIFTableImporter(tables, dataset, units, datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
