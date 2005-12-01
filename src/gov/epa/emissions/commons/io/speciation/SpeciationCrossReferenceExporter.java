package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.PrintWriter;
import java.util.StringTokenizer;

public class SpeciationCrossReferenceExporter extends SpeciationProfileExporter {
    private Dataset dataset;

    private Datasource datasource;

    private FileFormat fileFormat;
    
    public SpeciationCrossReferenceExporter(Dataset dataset, Datasource datasource, FileFormat fileFormat) {
        super(dataset, datasource, fileFormat);
        this.dataset = dataset;
        this.datasource = datasource;
        this.fileFormat = fileFormat;
    }
    
    protected void writeHeaders(PrintWriter writer, Dataset dataset) {
        String header = dataset.getDescription();
        String lasttoken = null;

        if(header != null){
            StringTokenizer st = new StringTokenizer(header, "#");
            while (st.hasMoreTokens()){
                lasttoken = st.nextToken();
                int index = lasttoken.indexOf("/POINT DEFN/");
                if(index <= 0)
                    writer.print("#" + lasttoken);
                else
                    writer.print("#" + lasttoken.substring(0,index)
                            + lasttoken.substring(index));
            }
        }
    }
    
}
