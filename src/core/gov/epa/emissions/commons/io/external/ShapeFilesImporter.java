package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class ShapeFilesImporter extends AbstractExternalFilesImporter {

    public ShapeFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        this(folder, filePatterns, dataset, datasource, sqlDataTypes, null);
    }

    public ShapeFilesImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        super(folder, filePatterns, dataset, datasource, sqlDataTypes, factory);
        importerName = "Shape Files Importer";
    }
}
