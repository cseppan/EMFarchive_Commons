package gov.epa.emissions.commons.io;

import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class LongFormatter implements ColumnFormatter {

    public final Format FORMAT = new Format("%10d");// FIXME: precision for long

    // FIXME: what about -9 ? Same for other formatters.
    public String format(String name, ResultSet data) throws SQLException {
        return FORMAT.format(data.getInt(name));
    }

}
