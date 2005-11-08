package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAActivityImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAActivityImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        delegate = new IDAImporter(dataset, datasource, sqlDataTypes);
        this.sqlDataTypes = sqlDataTypes;
    }

    public void preCondition(File folder, String filePattern) throws Exception {
        delegate.preImport(new IDAActivityFileFormat(sqlDataTypes));
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run();
    }
}
