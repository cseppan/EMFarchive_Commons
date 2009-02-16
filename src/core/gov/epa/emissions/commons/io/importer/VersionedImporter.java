package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.TableModifier;

import java.sql.Timestamp;
import java.util.Date;

public class VersionedImporter implements Importer {
    private Importer delegate;

    private Dataset dataset;

    private Datasource datasource;

    private Date lastModifiedDate;
    
    public VersionedImporter(Importer delegate, Dataset dataset, DbServer dbServer, Date lastModifiedDate) {
        this.delegate = delegate;
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
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
        TableModifier modifier = new TableModifier(datasource,"versions");
        String[] data = { null, dataset.getId() + "", "0", "Initial Version", "", "true", new Timestamp(lastModifiedDate.getTime())+""};
        modifier.insertOneRow(data);
    }
}
