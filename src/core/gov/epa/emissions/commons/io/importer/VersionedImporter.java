package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.util.utils;

import java.util.Date;

import org.hibernate.Session;

public class VersionedImporter implements Importer {
    private Importer delegate;

    private Dataset dataset;

    private Datasource datasource;

    private Date lastModifiedDate;
    
    public VersionedImporter(Importer delegate, Dataset dataset, DbServer dbServer, Date lastModifiedDate) {
        this.delegate = delegate;
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource(); 
// completed: datasource is not used anywhere else except addVersionZeroEntryToVersionsTable
        this.lastModifiedDate = lastModifiedDate;
    }
    
    //NOTE: need to access the importer to get external sources
    public Importer getWrappedImporter() {
        return delegate;
    }

    public void run() throws ImporterException {
        delegate.run();
        try {
            addVersionZeroEntryToVersionsTable(datasource, dataset);
        } catch (Exception e) {
            throw new ImporterException("Could not add Version Zero entry to the Versions Table." + e.getMessage());
        }
//NOTE: should let the calling function to close it explicitly for ease of db management 2/13/2008
//        } finally {
//            try {
//                this.dbServer.disconnect();
//            } catch (Exception exc) {
//                throw new ImporterException("Could not disconnect db server: " + exc.getMessage());
//            }
//        }
    }
    
    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws Exception {

//      TableModifier modifier = new TableModifier(datasource,"versions"); 
//      String[] data = { null, dataset.getId() + "", "0", "Initial Version", "", "true", new Timestamp(lastModifiedDate.getTime())+""};
//        modifier.insertOneRow(data);
        
        
        Session session = utils.getHibernateSession();

        Version defaultZeroVersion = new Version(0);
        defaultZeroVersion.setName("Initial Version");
        defaultZeroVersion.setPath("");
        defaultZeroVersion.setDatasetId(dataset.getId());
        defaultZeroVersion.setLastModifiedDate(lastModifiedDate);
        defaultZeroVersion.setFinalVersion(true);
        defaultZeroVersion.setDescription("");
        
        try {
            utils.add(defaultZeroVersion, session);
        } catch (Exception e) {
            throw new Exception("Could not add version 0 entry for dataset " + dataset.getName() + " into versions table: " + e.getMessage());
        } finally {
            session.close();
        }
    }
}
