package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ModelReadyEmissionsFilesImporter extends AbstractExternalFilesImporter {

    public ModelReadyEmissionsFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataType) throws ImporterException {
        this(folder, filePatterns, dataset, datasource, sqlDataType, null);
    }

    public ModelReadyEmissionsFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataType, DataFormatFactory factory) throws ImporterException {
        super(folder, filePatterns, dataset, datasource, sqlDataType, factory);
        importerName = "Model Ready Emissions Files Importer";
    }

}
