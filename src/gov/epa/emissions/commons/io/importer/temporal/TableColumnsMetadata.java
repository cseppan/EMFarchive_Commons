package gov.epa.emissions.commons.io.importer.temporal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlDataType;

public class TableColumnsMetadata implements ColumnsMetadata {

    private String datasetIdType;

    private String datasetIdColName;

    private ColumnsMetadata base;

    public TableColumnsMetadata(ColumnsMetadata base, SqlDataType sqlType) {
        this.base = base;

        datasetIdType = sqlType.getLong();
        datasetIdColName = "Dataset_Id";
    }

    public String[] colNames() {
        return add(datasetIdColName, base.colNames());
    }

    public String[] colTypes() {
        return add(datasetIdType, base.colTypes());
    }

    private String[] add(String element, String[] array) {
        List all = new ArrayList();
        all.add(element);
        all.addAll(Arrays.asList(array));

        return (String[]) all.toArray(new String[0]);
    }

    public int[] widths() {
        return base.widths();
    }

}
