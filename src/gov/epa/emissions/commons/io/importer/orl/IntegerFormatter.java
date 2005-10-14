package gov.epa.emissions.commons.io.importer.orl;

import gov.epa.emissions.commons.io.ColumnFormatter;

import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class IntegerFormatter implements ColumnFormatter {

    public final Format FORMAT = new Format("%5d");

    public String format(String name, ResultSet data) throws SQLException {
        return FORMAT.format(data.getInt(name));
    }

}
