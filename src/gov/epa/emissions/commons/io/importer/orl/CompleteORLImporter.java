package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetType;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLTableType;
import gov.epa.emissions.commons.io.importer.SummaryTableCreator;

/**
 * The importer for ORL (One Record per Line) format text files.
 */
public class CompleteORLImporter extends BaseORLImporter {

    public CompleteORLImporter(DbServer dbServer, boolean annualNotAverageDaily, ORLDatasetTypesFactory typesFactory,
            DatasetType datasetType) {
        super(dbServer, annualNotAverageDaily, typesFactory, datasetType);
    }

    public void run(Dataset dataset) throws Exception {
        super.run(dataset);
        createSummaryTable(dataset);
    }

    private void createSummaryTable(Dataset dataset) throws Exception {
        ORLTableType tableType = tableTypes.type(dataset.getDatasetType());
        Table table = dataset.getTable(tableType.base());
        String summaryTable = (String) dataset.getTablesMap().get(tableType.summary());

        SummaryTableCreator modifier = new SummaryTableCreator(dbServer.getEmissionsDatasource(), dbServer
                .getReferenceDatasource(), typesFactory);
        modifier.createORLSummaryTable(dataset.getDatasetType(), table.getName(), summaryTable, annualNotAverageDaily);
    }

}
