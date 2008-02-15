package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.other.SMKReportExporter;

import java.sql.Types;

public class SpeciationComboProfileExporter extends SMKReportExporter {
    public SpeciationComboProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, sqlDataTypes, optimizedBatchSize);
        setup();
    }

    public SpeciationComboProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes sqlDataTypes,
            DataFormatFactory formatFactory, Integer optimizedBatchSize) {
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
            return value;

        if (colType == Types.DOUBLE)
            return value;

        if (colType == Types.FLOAT)
            return value;

        if (colType == Types.INTEGER)
            return value;

        if (colType == Types.NUMERIC)
            return value;

        if (colType == Types.REAL)
            return value;

        if (colType == Types.SMALLINT)
            return value;

        return "\"" + value + "\"";
    }
}
