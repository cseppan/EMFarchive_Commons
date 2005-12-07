package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDANonPointNonRoadImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDANonPointNonRoadImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(dataset, datasource, sqlDataTypes);
        setup(file);
    }

    private void setup(File file) throws ImporterException {
        delegate.setup(file, new IDANonPointNonRoadFileFormat(sqlDataTypes));
    }

    public void run() throws ImporterException {
        delegate.run();
    }
}
