package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.ColumnFormatter;
import gov.epa.emissions.commons.io.StringFormat;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringFormatter implements ColumnFormatter {

    private StringFormat format;

    public StringFormatter(int size) {
        format = new StringFormat(size);
    }

    public String format(String name, ResultSet data) throws SQLException {
        String value = data.getString(name);
        String evalValue = (value == null) ? "-9" : value;

        return format.format(evalValue);
    }

}
