package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.other.SMKReportExporter;

public class CSVExporter extends SMKReportExporter {

    public CSVExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, Integer optimizedBatchSize) {
        super(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        setup();
    }

    public CSVExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes, DataFormatFactory formatFactory,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, sqlDataTypes, formatFactory, optimizedBatchSize);
        setup();
    }

    private void setup() {
        super.setDelimiter(",");
    }

    protected String formatValue(String[] cols, int index, String value) {
        return "\"" + value + "\"";
    }

}
