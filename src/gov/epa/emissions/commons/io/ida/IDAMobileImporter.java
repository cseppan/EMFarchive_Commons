package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public class IDAMobileImporter implements Importer {

    private IDAImporter delegate;

    private SqlDataTypes sqlDataTypes;

    public IDAMobileImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws Exception {
        this.sqlDataTypes = sqlDataTypes;
        delegate = new IDAImporter(dataset, datasource, sqlDataTypes);
        setup(file);
    }
    
    private void setup(File file) throws Exception {
       delegate.setup(file, new IDAMobileFileFormat(sqlDataTypes));
        
    }

    public void run() throws ImporterException {
            delegate.run();


    }

    

}
