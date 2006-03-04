package gov.epa.emissions.commons.io;


import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class RealFormatter implements ColumnFormatter {

    // TODO: review the precision
    public final Format FORMAT = new Format("%14.7e");

    public String format(String name, ResultSet data) throws SQLException {
        if (data.getString(name) == null || data.getFloat(name) == -9)
            return "";

        return FORMAT.format(data.getDouble(name));
    }
}
