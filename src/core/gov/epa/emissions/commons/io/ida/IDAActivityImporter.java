package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAActivityImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAActivityImporter(File file, Dataset dataset, Datasource emissionDatasource, Datasource referenceDatasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        delegate = new IDAImporter(dataset, emissionDatasource, referenceDatasource, sqlDataTypes);
        this.sqlDataTypes = sqlDataTypes;
        setup(file);
    }

    private void setup(File file) throws ImporterException {
        delegate.setup(file, new IDAActivityFileFormat(sqlDataTypes));
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
