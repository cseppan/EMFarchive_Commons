package gov.epa.emissions.commons.io.csv;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.other.SMKReportExporter;

import java.sql.Types;

public class CSVExporter extends SMKReportExporter {

    public CSVExporter(Dataset dataset, String rowFilter,  DbServer dbServer, Integer optimizedBatchSize) {
        super(dataset, rowFilter, dbServer, optimizedBatchSize);
        setup();
    }

    public CSVExporter(Dataset dataset, String rowFilter, DbServer dbServer, DataFormatFactory formatFactory,
            Integer optimizedBatchSize) {
        super(dataset, rowFilter, dbServer, formatFactory, optimizedBatchSize);
        setup();
    }

    private void setup() {
        super.setDelimiter(",");
    }

    protected String formatValue(String[] cols, int colType, int index, String value) {
        if (colType == Types.BIGINT)
            return value;
        
        if (colType == Types.DECIMAL)
            return new Double(Double.valueOf(value)).toString();
        
        if (colType == Types.DOUBLE)
            return new Double(Double.valueOf(value)).toString();
        
        if (colType == Types.FLOAT)
            return new Double(Double.valueOf(value)).toString();
        
        if (colType == Types.INTEGER)
            return value;
        
        if (colType == Types.NUMERIC)
            return new Double(Double.valueOf(value)).toString();
        
        if (colType == Types.REAL)
            return new Double(Double.valueOf(value)).toString();
        
        if (colType == Types.SMALLINT)
            return value;
        
        return "\"" + value + "\"";
    }

}
