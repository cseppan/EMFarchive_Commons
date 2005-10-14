package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.OptionalColumnsMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PointTemporalReferenceColumnsMetadata implements OptionalColumnsMetadata {

    private String[] minTypes;

    private String[] minNames;

    private String[] optionalNames;

    private String[] optionalTypes;

    public PointTemporalReferenceColumnsMetadata(SqlDataTypes types) {
        minTypes = new String[] { types.stringType(10), types.intType(), types.intType(), types.intType() };
        String charType = types.charType();
        optionalTypes = new String[] { charType, types.intType(), charType, charType,
                charType, charType, charType, charType };

        minNames = new String[] { "SCC", "Monthly_Code", "Weekly_Code", "Diurnal_Code" };
        optionalNames = new String[] { "Pollutants", "CSC_Code", "Plant_Id", "Characteristic_1", "Characteristic_2",
                "Characteristic_3", "Characteristic_4", "Characteristic_5" };

    }

    public int[] widths() {
        return null;
    }

    public String[] colNames() {
        return asArray(minNames, optionalNames);
    }

    public String[] colTypes() {
        return asArray(minTypes, optionalTypes);
    }

    private String[] asArray(String[] min, String[] optional) {
        List list = new ArrayList();
        list.addAll(Arrays.asList(min));
        list.addAll(Arrays.asList(optional));

        return (String[]) list.toArray(new String[0]);
    }

    public String[] optionalTypes() {
        return optionalTypes;
    }

    public String[] minTypes() {
        return minTypes;
    }

    public String identify() {
        return "Point - Temporal Reference";
    }
}
