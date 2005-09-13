package gov.epa.emissions.commons.io.exporter.orl;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

/**
 * Control Efficiency percentage (CEFF)
 */
public class CeffFormatter implements Formatter {

    public static final Format FORMAT = new Format("%6.2f");

    public void format(ResultSet data, PrintWriter writer) throws SQLException {
        if (data.getString("CEFF") == null)
            writer.print("-9");
        else
            writer.print(FORMAT.format(data.getDouble("CEFF")));

    }

}
