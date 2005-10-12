package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.Table;
import gov.epa.emissions.commons.io.importer.ORLDatasetTypesFactory;
import gov.epa.emissions.commons.io.importer.ORLTableType;
import gov.epa.emissions.commons.io.importer.SummaryTableCreator;

import java.io.File;

/**
 * The importer for ORL (One Record per Line) format text files.
 */
public class CompleteORLImporter extends BaseORLImporter {

    public CompleteORLImporter(DbServer dbServer, boolean annualNotAverageDaily, ORLDatasetTypesFactory typesFactory) {
        super(dbServer, annualNotAverageDaily, typesFactory);
    }

    public void run(File[] files, Dataset dataset, boolean overwrite) throws Exception {
        super.run(files, dataset, overwrite);
        createSummaryTable(dataset, overwrite);
    }

    private void createSummaryTable(Dataset dataset, boolean overwrite) throws Exception {
        ORLTableType tableType = tableTypes.type(dataset.getDatasetType());
        Table table = dataset.getTable(tableType.base());
        String summaryTable = (String) dataset.getTablesMap().get(tableType.summary());

        SummaryTableCreator modifier = new SummaryTableCreator(dbServer.getEmissionsDatasource(), dbServer
                .getReferenceDatasource(), typesFactory);
        modifier.createORLSummaryTable(dataset.getDatasetType(), table.getName(), summaryTable, overwrite,
                annualNotAverageDaily);
    }

}
