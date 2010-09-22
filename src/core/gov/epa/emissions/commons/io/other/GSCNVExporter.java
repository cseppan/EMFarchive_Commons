package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

public class GSCNVExporter extends GenericExporter {
    public GSCNVExporter(Dataset dataset, String rowFilters, DbServer dbServer, Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, fileFormat(dbServer.getSqlDataTypes()), new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public GSCNVExporter(Dataset dataset, String rowFilters, DbServer dbServer, 
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        super(dataset, rowFilters, dbServer, fileFormat(dbServer.getSqlDataTypes()), dataFormatFactory, optimizedBatchSize);
    }

    private static FileFormat fileFormat(SqlDataTypes sqlDataTypes) {
        return new GSCNVFileFormat(sqlDataTypes);
    }
}