package gov.epa.emissions.commons.io.speciation;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class SpeciationCrossRefFileFormat implements FileFormat {
  
    private Column[] columns;
    
    public SpeciationCrossRefFileFormat(SqlDataTypes type){
        columns = createCols(type);
    }
    
    public String identify() {
        return "Speciation Cross-Reference";
    }

    public Column[] cols() {
        return columns;
    }
    
    private Column[] createCols(SqlDataTypes types) {
        List columns = new ArrayList();
        //FIXME: String type uses 32 as the number of characters assuming it is big enough to 
        //hold the relevant fields
        columns.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        columns.add(new Column("CODE", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("POLLUTANT", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("FIPS", types.intType(), new IntegerFormatter()));
        columns.add(new Column("MACT", types.stringType(6), new StringFormatter(6)));
        columns.add(new Column("SIC", types.intType(), new IntegerFormatter()));
        columns.add(new Column("PLANTID", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("POINTID", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("STACKID", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("SEGMENTID", types.stringType(32), new StringFormatter(32)));

        return (Column[]) columns.toArray(new Column[0]);
    }
}
