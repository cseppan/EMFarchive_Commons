package gov.epa.emissions.commons.io;

import java.sql.ResultSet;
import java.sql.SQLException;

import corejava.Format;

public class RealFormatter implements ColumnFormatter {

    public final Format FORMAT = new Format("%14.7e");
    
    private int spaces;
    
    private int width;
    
    public RealFormatter(int width, int spaces) {
        this.spaces = spaces;
        this.width = width;
    }

    public RealFormatter() {
        this.spaces = 0;
        this.width = 0;
    }

    public String format(String name, ResultSet data) throws SQLException {
        if (data.getString(name) == null || data.getFloat(name) == -9)
            return getSpaces(this.width + this.spaces);
        
        int prefix = this.width - data.getString(name).length();

        return getSpaces(prefix) + data.getString(name) + getSpaces(this.spaces);
    }
    
    public String getSpaces(int n)
    {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < n; i++)
            buf.append(" ");
        return buf.toString();
    }
}
