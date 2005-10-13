package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OptionalColumnsTableMetadata implements ColumnsMetadata {

    private List colNames;

    private List colTypes;

    private OptionalColumnsMetadata base;

    public OptionalColumnsTableMetadata(OptionalColumnsMetadata base, SqlDataTypes sqlType) {
        this.base = base;

        colNames = new ArrayList();
        colNames.add(key());
        colNames.addAll(Arrays.asList(base.colNames()));
        colNames.add("Comments");

        colTypes = new ArrayList();
        colTypes.add(sqlType.longType());
        colTypes.addAll(Arrays.asList(base.colTypes()));
        colTypes.add(sqlType.stringType(128));
    }

    public String[] colNames() {
        return (String[]) colNames.toArray(new String[0]);
    }

    public String[] colTypes() {
        return (String[]) colTypes.toArray(new String[0]);
    }

    public String key() {
        return "Dataset_Id";
    }

    public int[] widths() {
        return null;
    }

    // FIXME: handle inline comments
    public void addDefaultValuesForOptionals(List data) {
//        String last = (String) data.get(data.size() - 1);
        int fillers = colTypes.size() - data.size() - 1;
        String[] optionalTypes = base.optionalTypes();
        for (int i = optionalTypes.length - fillers; i < optionalTypes.length; i++) {
            data.add("");// default values
        }
        data.add("");// comment
    }

}
