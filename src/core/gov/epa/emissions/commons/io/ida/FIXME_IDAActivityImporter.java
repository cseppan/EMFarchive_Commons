package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.File;

public class FIXME_IDAActivityImporter implements Importer {

    private IDAImporter delegate;

    public FIXME_IDAActivityImporter(File folder, String[] fileNames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes) throws ImporterException {
        this(folder, fileNames, dataset, dbServer, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public FIXME_IDAActivityImporter(File folder, String[] fileNames, Dataset dataset, DbServer dbServer,
            SqlDataTypes sqlDataTypes, DataFormatFactory factory) throws ImporterException {
        delegate = new IDAImporter(dataset, dbServer, sqlDataTypes);
        delegate.setup(folder, fileNames, new IDAActivityFileFormat(sqlDataTypes), factory);
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
