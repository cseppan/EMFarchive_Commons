package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.StringTokenizer;

public class TemporalReferenceExporter extends GenericExporter {

    public TemporalReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        this(dataset, dbServer, types, new NonVersionedDataFormatFactory(), optimizedBatchSize);
    }

    public TemporalReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory dataFormatFactory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new TemporalReferenceFileFormat(types, dataFormatFactory.defaultValuesFiller()),
                dataFormatFactory, optimizedBatchSize);
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) throws SQLException {
        String header = dataset.getDescription();
        String lasttoken = null;
        String lastHeaderLine = null;

        if (header != null && !header.trim().isEmpty()) {
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()) {
                lasttoken = st.nextToken();
                int index = lasttoken.indexOf("/POINT DEFN/");
                if (index < 0)
                    writer.print("#" + lasttoken);
                else
                    lastHeaderLine = lasttoken;
            }

            printExportInfo(writer);

            if (lastHeaderLine != null)
                writer.print(lastHeaderLine);
        }
    }

}
