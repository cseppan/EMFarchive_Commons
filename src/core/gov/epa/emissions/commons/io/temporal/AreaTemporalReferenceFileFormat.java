package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.FillDefaultValues;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AreaTemporalReferenceFileFormat implements FileFormatWithOptionalCols {
    private SqlDataTypes types;

    private PointTemporalReferenceFileFormat base;

    private FillDefaultValues filler;

    public AreaTemporalReferenceFileFormat(SqlDataTypes types) {
        this.types = types;
        this.base = new PointTemporalReferenceFileFormat(types);
        filler = new FillDefaultValues(this);
    }

    public String identify() {
        return "Area - Temporal Reference";
    }

    public Column[] cols() {
        Column[] minCols = minCols();
        Column[] optionalCols = optionalCols();

        return asArray(minCols, optionalCols);
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List list = new ArrayList();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return (Column[]) list.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        Column pollutants = new Column("Pollutants", types.stringType(32), new StringFormatter(32));
        Column fips = new Column("FIPS", types.intType(), new IntegerFormatter());

        return new Column[] { pollutants, fips };
    }

    public Column[] minCols() {
        return base.minCols();
    }

    public void fillDefaults(List data, long datasetId) {
        filler.fillDefaults(data, datasetId);
    }
}
