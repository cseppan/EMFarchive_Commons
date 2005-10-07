package gov.epa.emissions.commons.io.importer.temporal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import gov.epa.emissions.commons.db.SqlTypeMapper;

public class MonthlyTableColumnsMetadata extends MonthlyColumnsMetadata {

    private String datasetIdType;

    private String datasetIdColName;

    public MonthlyTableColumnsMetadata(SqlTypeMapper typeMapper) {
        super(typeMapper);

        datasetIdType = typeMapper.getLong();
        datasetIdColName = "Dataset_Id";
    }

    public String[] colNames() {
        return add(datasetIdColName, super.colNames());
    }

    public String[] colTypes() {
        return add(datasetIdType, super.colTypes());
    }

    private String[] add(String element, String[] array) {
        List all = new ArrayList();
        all.add(element);
        all.addAll(Arrays.asList(array));

        return (String[]) all.toArray(new String[0]);
    }

}
