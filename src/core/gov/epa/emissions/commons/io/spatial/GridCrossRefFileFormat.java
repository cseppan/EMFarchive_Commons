package gov.epa.emissions.commons.io.spatial;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

public class GridCrossRefFileFormat implements FileFormat {
    private SqlDataTypes types;

    public GridCrossRefFileFormat(SqlDataTypes types) {
        this.types = types;
    }

    public String identify() {
        return "Gridding Cross Reference";
    }

    public Column[] cols() {
        Column fips = new Column("FIPS", types.stringType(32), new StringFormatter(32));
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column code = new Column("CODE", types.intType(), new IntegerFormatter());
        
        return new Column[] { fips, scc, code };
    }
}
