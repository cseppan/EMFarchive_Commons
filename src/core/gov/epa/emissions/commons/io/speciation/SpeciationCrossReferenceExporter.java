package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.generic.GenericExporter;

import java.io.PrintWriter;
import java.util.StringTokenizer;

public class SpeciationCrossReferenceExporter extends GenericExporter {
    
    public SpeciationCrossReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types, Integer optimizedBatchSize) {
        super(dataset, dbServer, new SpeciationCrossRefFileFormat(types), optimizedBatchSize);
    }
    
    public SpeciationCrossReferenceExporter(Dataset dataset, DbServer dbServer, SqlDataTypes types,
            DataFormatFactory factory, Integer optimizedBatchSize) {
        super(dataset, dbServer, new SpeciationCrossRefFileFormat(types), factory, optimizedBatchSize);
    }
    
    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String header = dataset.getDescription();
        String lasttoken = null;

        if(header != null){
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()){
                lasttoken = st.nextToken();
                int index = lasttoken.indexOf("/POINT DEFN/");
                if(index < 0)
                    writer.print("#" + lasttoken);
                else if(index == 0)
                    writer.print(lasttoken);
                else
                    writer.print("#" + lasttoken.substring(0,index)
                            + lasttoken.substring(index));
            }
        }
    }
    
}
