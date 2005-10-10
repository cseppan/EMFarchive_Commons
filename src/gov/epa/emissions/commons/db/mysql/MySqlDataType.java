package gov.epa.emissions.commons.db.mysql;

import gov.epa.emissions.commons.db.SqlDataTypes;

public class MySqlDataType implements SqlDataTypes {

    public String getType(String name, String genericType, int width) {
        if (genericType.equals("C"))
            return "VARCHAR(" + width + ")";
        if (genericType.equals("I"))
            return "INT";
        // if the type is "N" that means number, then check if there is either
        // "date" or "time" contained in the name.. if so.. return appropriate
        // type
        if (genericType.equals("N")) {
            // if the name contains date
            if (name.indexOf("date") > -1) {
                return "DATE";
            }
            if (name.indexOf("time") > -1) {
                return "INT";
            }
            return "DOUBLE";
        }
        return null;
    }

    public String getString(int size) {
        return "VARCHAR(" + size + ")";
    }

    public String getInt() {
        return "INT";
    }

    public String getLong() {
        return "INT";// FIXME: verify
    }

    public String getReal() {
        return "DOUBLE";// FIXME: verify precision
    }

    public String smallInt() {
        return "SMALLINT";
    }

}
