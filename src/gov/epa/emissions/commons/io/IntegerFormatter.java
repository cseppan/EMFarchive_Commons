package gov.epa.emissions.commons.io;


import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class IntegerFormatter implements ColumnFormatter {

    public final Format FORMAT = new Format("%5d");

    // FIXME: what about -9 ? Same for other formatters.
    public String format(String name, ResultSet data) throws SQLException {
        return FORMAT.format(data.getInt(name));
    }

}
