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

    // FIXME: rework this mess
    public void addDefaultValuesForOptionals(List data) {
        if (!includesComment(data))
            addDefaultValuesWithComment(data);
        else
            addDefaultValues(data);
    }

    private void addDefaultValues(List data) {
        int optionalCount = optionalCount(data);
        int toAdd = toAdd(optionalCount);
        int insertAt = insertAt(optionalCount);

        for (int i = 0; i < toAdd; i++)
            data.add(insertAt + i, "");
    }

    private int insertAt(int optionalCount) {
        return base.minTypes().length + 1 + optionalCount;
    }

    private int toAdd(int optionalCount) {
        return base.optionalTypes().length - optionalCount;
    }

    private int optionalCount(List data) {
        return data.size() - (2 + base.minTypes().length);
    }

    private void addDefaultValuesWithComment(List data) {
        String[] optionalTypes = base.optionalTypes();
        int toAdd = colTypes.size() - data.size() - 1;

        for (int i = optionalTypes.length - toAdd; i < optionalTypes.length; i++)
            data.add("");
        data.add("");// comment
    }

    private boolean includesComment(List data) {
        String last = (String) data.get(data.size() - 1);
        return last.startsWith("!");
    }

    public String identify() {
        return base.identify();
    }

}
