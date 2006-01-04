package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;

import java.io.PrintWriter;
import java.util.StringTokenizer;

public class TemporalReferenceExporter extends GenericExporter {

    public TemporalReferenceExporter(Dataset dataset, Datasource datasource, SqlDataTypes types) {
        this(dataset, datasource, types, new NonVersionedDataFormatFactory());
    }

    public TemporalReferenceExporter(Dataset dataset, Datasource datasource, SqlDataTypes types,
            DataFormatFactory dataFormatFactory) {
        super(dataset, datasource, new TemporalReferenceFileFormat(types), dataFormatFactory);
    }

    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String header = dataset.getDescription();
        String lasttoken = null;

        if (header != null) {
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()) {
                lasttoken = st.nextToken();
                int index = lasttoken.indexOf("/POINT DEFN/");
                if (index < 0)
                    writer.print("#" + lasttoken);
                else
                    writer.print(lasttoken);
            }
        }
    }

}
