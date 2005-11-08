package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDANonPointImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDANonPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(dataset, datasource, sqlDataTypes);
    }

    public void preCondition(File folder, String filePattern) throws ImporterException {
        delegate.preImport(new IDANonPointFileFormat(sqlDataTypes));
    }

    public void run(Dataset dataset2) throws ImporterException {
        delegate.run();
    }
}
