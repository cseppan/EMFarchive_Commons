package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.DataQuery;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.generic.GenericExporter;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class SpeciationCrossReferenceExporter extends GenericExporter {
    
    private FileFormat fileFormat;
    
    private SqlDataTypes types;
    
    public SpeciationCrossReferenceExporter(Dataset dataset, Datasource datasource, 
            SqlDataTypes types) {
        super(dataset, datasource,  new SpeciationCrossRefFileFormat(types));
        this.fileFormat = new SpeciationCrossRefFileFormat(types);
        this.types = types;
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
    
    protected void writeData(PrintWriter writer, Dataset dataset, Datasource datasource) throws SQLException {
        DataQuery q = datasource.query();
        InternalSource source = dataset.getInternalSources()[0];
        
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(fileFormat.cols()));

        Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        cols.add(inlineComments);
        
        ResultSet data = q.selectAll(source.getTable());
        while (data.next())
            writeRecord((Column[]) cols.toArray(new Column[0]), data, writer);
    }

    
}
