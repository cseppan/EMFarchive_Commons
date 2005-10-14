package gov.epa.emissions.commons.io;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class Column {

    public static final Format FORMAT = new Format("%5d");

    public void format(ResultSet data, PrintWriter writer) throws SQLException {
        writer.print(FORMAT.format(data.getInt("FIPS")));
    }

}
