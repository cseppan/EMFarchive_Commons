package gov.epa.emissions.commons.io.importer.temporal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public class TableColumnsMetadata implements ColumnsMetadata {

    private ColumnsMetadata base;

    private List colNames;

    private List colTypes;

    public TableColumnsMetadata(ColumnsMetadata base, SqlDataTypes sqlType) {
        this.base = base;

        colNames = new ArrayList();
        colNames.add("Dataset_Id");
        colNames.addAll(Arrays.asList(base.colNames()));
        colNames.add("Comments");

        colTypes = new ArrayList();
        colTypes.add(sqlType.getLong());
        colTypes.addAll(Arrays.asList(base.colTypes()));
        colTypes.add(sqlType.getString(128));
    }

    public String[] colNames() {
        return (String[]) colNames.toArray(new String[0]);
    }

    public String[] colTypes() {
        return (String[]) colTypes.toArray(new String[0]);
    }

    public int[] widths() {
        return base.widths();
    }

}
