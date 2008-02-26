package gov.epa.emissions.commons.io.speciation;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

public class SpeciationComboProfileExporter extends GenericExporter {

    public SpeciationComboProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            Integer optimizedBatchSize) {
        super(dataset, dbServer, new SpeciationComboProfileFileFormat(types), optimizedBatchSize);
    }

    public SpeciationComboProfileExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new SpeciationComboProfileFileFormat(types), factory, optimizedBatchSize);
    }

    // NOTE: overwrite so that trailing blank columns are truncated
    protected void writeDataCols(String[] cols, ResultSet data, PrintWriter writer) throws SQLException {
        int endCol = cols.length - 1;
        int colCheckNum = startColNumber - 1;
        boolean continueCheck = true;
        String toWrite = "";

        for (int i = endCol; i > colCheckNum; i--) {
            String value = formatValue(i, data);

            if (continueCheck && (value == null || value.isEmpty()))
                continue;
            
            continueCheck = false;
            toWrite = value + (toWrite.isEmpty() ? "" : delimiter + toWrite);
        }

        writer.write(toWrite);
    }

}
