package gov.epa.emissions.commons.io.external;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class MeteorologyFilesImporter extends AbstractExternalFilesImporter {

    public MeteorologyFilesImporter(File folder, String[] filePatterns, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataType) throws ImporterException {
        this(folder, filePatterns, dataset, dbServer, sqlDataType, null);
    }

    public MeteorologyFilesImporter(File folder, String[] filePatterns, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataType, DataFormatFactory factory) throws ImporterException {
        super(folder, filePatterns, dataset, dbServer, sqlDataType, factory);
        importerName = "Meteorology Files Importer";
    }

}
