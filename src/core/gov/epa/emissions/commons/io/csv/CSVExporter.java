package gov.epa.emissions.commons.io.csv;

import java.sql.Types;

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
