package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class DelimitedColumnsMetadata implements ColumnsMetadata {

    private String identifier;

    private Column[] columns;

    public DelimitedColumnsMetadata(String identifier, int cols, SqlDataTypes typeMapper) {
        columns = createCols(typeMapper, cols);
        this.identifier = identifier;
    }

    public int[] widths() {
        return null;
    }

    public String[] colTypes() {
        Column[] cols = cols();

        List sqlTypes = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            sqlTypes.add(cols[i].sqlType());
        }

        return (String[]) sqlTypes.toArray(new String[0]);
    }

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String identify() {
        return identifier;
    }

    public Column[] cols() {
        return columns;
    }

    private Column[] createCols(SqlDataTypes types, int cols) {
        List columns = new ArrayList();

        for (int i = 0; i < cols; i++)
            columns.add(new Column(types.stringType(32), new StringFormatter(32), "Col_" + i));

        return (Column[]) columns.toArray(new Column[0]);
    }
}
