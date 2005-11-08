package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAPointImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAPointImporter(Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(dataset, datasource, sqlDataTypes);
    }

    public void preCondition(File folder, String filePattern) throws Exception {
        IDAFileFormat fileFormat = new IDAPointFileFormat(sqlDataTypes);
        delegate.preImport(fileFormat);
    }

    public void run(Dataset dataset) throws ImporterException {
        delegate.run();

    }

}
