package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.TableModifier;

public class VersionedImporter implements Importer {
    private Importer delegate;

    private Dataset dataset;

    private Datasource datasource;

    public VersionedImporter(Importer delegate, Dataset dataset, DbServer dbServer) {
        this.delegate = delegate;
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
    }

    public void run() throws ImporterException {
        delegate.run();
        try {
            addVersionZeroEntryToVersionsTable(datasource, dataset);
        } catch (Exception e) {
            throw new ImporterException("Could not add Version Zero entry to the Versions Table." + e.getMessage());
        }
    }
    
    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws Exception {
        TableModifier modifier = new TableModifier(datasource,"versions");
        String[] data = { null, dataset.getId() + "", "0", "Initial Version", "", "true", null };
        modifier.insertOneRow(data);
    }
}
