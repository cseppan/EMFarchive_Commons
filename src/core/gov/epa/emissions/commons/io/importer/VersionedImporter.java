package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;

import java.sql.SQLException;

public class VersionedImporter implements Importer {
    private Importer delegate;

    private Dataset dataset;

    private Datasource datasource;

    public VersionedImporter(Importer delegate, Dataset dataset, DbServer dbServer) {
        this.delegate = delegate;
        this.dataset = dataset;
        this.datasource = dbServer.getEmissionsDatasource();
    }

    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        String[] data = { null, dataset.getId() + "", "0", "Initial Version", "", "true", null };
        modifier.insertRow("versions", data);
    }

    public void run() throws ImporterException {
        delegate.run();
        try {
            addVersionZeroEntryToVersionsTable(datasource, dataset);
        } catch (SQLException e) {
            throw new ImporterException("Could not add Version Zero entry to the Versions Table." + e.getMessage());
        }
    }
}
