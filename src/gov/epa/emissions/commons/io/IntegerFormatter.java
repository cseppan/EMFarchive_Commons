package gov.epa.emissions.commons.io;

import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class IntegerFormatter implements ColumnFormatter {

    public final Format FORMAT = new Format("%5d");

    public String format(String name, ResultSet data) throws SQLException {
        if (data.getString(name) == null || data.getFloat(name) == -9)
            return "-9";

        return FORMAT.format(data.getInt(name));
    }

}
