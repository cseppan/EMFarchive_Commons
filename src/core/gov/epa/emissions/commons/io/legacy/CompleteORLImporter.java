package gov.epa.emissions.commons.io.legacy;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.ImporterException;

/**
 * The importer for ORL (One Record per Line) format text files.
 */
public class CompleteORLImporter extends BaseORLImporter {

    public CompleteORLImporter(DbServer dbServer, boolean annualNotAverageDaily, ORLDatasetTypesFactory typesFactory,
            DatasetType datasetType, Dataset dataset) {
        super(dbServer, annualNotAverageDaily, typesFactory, datasetType, dataset);
    }

    public void run() throws ImporterException {
        super.run();
        createSummaryTable(dataset);
    }

    private void createSummaryTable(Dataset dataset) throws ImporterException {
        ORLTableType tableType = tableTypes.type(dataset.getDatasetType());
        Table table = dataset.getTable(tableType.base());
        String summaryTable = (String) dataset.getTablesMap().get(tableType.summary());

        SummaryTableCreator modifier = new SummaryTableCreator(dbServer.getEmissionsDatasource(), dbServer
                .getReferenceDatasource(), typesFactory);
        try {
            modifier.createORLSummaryTable(dataset.getDatasetType(), table.getName(), summaryTable, annualNotAverageDaily);
        } catch (Exception e) {
            throw new ImporterException(e.getMessage());
        }
    }

}
