package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SMKReportFileFormat implements FileFormat, DelimitedFileFormat {

    private Column[] columns;
    
    private Map map;

    public SMKReportFileFormat(String[]cols, SqlDataTypes types) throws Exception {
        this.map = createMap(types);
        this.columns = createCols(cols, types);
    }
    
    public String identify() {
        return "SMOKE Report";
    }

    public Column[] cols() {
        return columns;
    }
    
    private Map createMap(SqlDataTypes types) {
        HashMap map = new HashMap();
        map.put("Date", new Column("DATE", types.stringType(32),new StringFormatter(32)));
        map.put("State", new Column("STATE", types.stringType(32),new StringFormatter(32)));
        map.put("Region", new Column("REGION", types.stringType(32),new StringFormatter(32)));
        map.put("Hour", new Column("HOUR", types.stringType(32),new StringFormatter(32)));
        map.put("SCC", new Column("SCC", types.stringType(32),new StringFormatter(32)));
        map.put("SCC Description", new Column("SCCDESC", types.stringType(128),new StringFormatter(128)));
        map.put("County", new Column("COUNTY", types.stringType(32),new StringFormatter(32)));
        
        return map;
    }
    
    private Column[] createCols(String[] cols, SqlDataTypes types) throws Exception { 
        List base = new ArrayList();
        
        for(int i = 0; i < cols.length; i++) {
            Column col = (Column)map.get(cols[i].trim());           
            if(col != null)
                base.add(i, col);
            else
                base.add(new Column(cols[i].trim(), types.realType(), new RealFormatter()));    
        }
        
        return (Column[]) base.toArray(new Column[0]);
    }
}
