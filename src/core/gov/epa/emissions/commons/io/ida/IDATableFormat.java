package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.TableFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IDATableFormat implements TableFormat {

    private FileFormat base;

    private SqlDataTypes types;

    public IDATableFormat(FileFormat base, SqlDataTypes types) {
        this.base = base;
        this.types = types;
    }

    public String key() {
        return "Dataset_Id";
    }

    public String identify() {
        return base.identify();
    }

    public Column[] cols() {
        List cols = new ArrayList();
        cols.addAll(Arrays.asList(base.cols()));

        //Column datasetId = new Column(key(), types.longType(), new LongFormatter());
        //cols.add(0, datasetId);

        Column state = new Column("STATE", types.stringType(2), new StringFormatter(2));
        cols.add(0, state);

        Column fips = new Column("FIPS", types.stringType(5), new StringFormatter(5));
        cols.add(1, fips);

        //Column inlineComments = new Column("Comments", types.stringType(128), new StringFormatter(128));
        //cols.add(inlineComments);

        return (Column[]) cols.toArray(new Column[0]);
    }

}
