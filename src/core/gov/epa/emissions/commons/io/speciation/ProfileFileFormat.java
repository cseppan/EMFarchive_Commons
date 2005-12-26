package gov.epa.emissions.commons.io.speciation;

import java.util.ArrayList;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class ProfileFileFormat implements FileFormat{

    private Column[] columns;
    
    public ProfileFileFormat(SqlDataTypes type){
        columns = createCols(type);
    }
    
    public String identify() {
        return "Chem Speciation Profile";
    }

    public Column[] cols() {
        return columns;
    }
    
    private Column[] createCols(SqlDataTypes types) {
        List columns = new ArrayList();

        columns.add(new Column("CODE", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("POLLUTANT", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("SPECIES", types.stringType(32), new StringFormatter(32)));
        columns.add(new Column("SPLIT", types.realType(), new RealFormatter()));
        columns.add(new Column("DIVISOR", types.realType(), new RealFormatter()));
        columns.add(new Column("MASSFRAC", types.realType(), new RealFormatter()));

        return (Column[]) columns.toArray(new Column[0]);
    }

}