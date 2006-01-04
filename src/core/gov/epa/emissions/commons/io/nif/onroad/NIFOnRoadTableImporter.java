package gov.epa.emissions.commons.io.nif.onroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.nif.NIFTableImporter;

public class NIFOnRoadTableImporter implements Importer {

    private NIFTableImporter delegate;

    public NIFOnRoadTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        this(tables, dataset, datasource, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public NIFOnRoadTableImporter(String[] tables, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes, 
            DataFormatFactory factory) throws ImporterException {
        delegate = new NIFTableImporter(tables, dataset, new NIFOnRoadTableDatasetTypeUnits(tables, datasource, sqlDataTypes, 
                factory), datasource);
    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
