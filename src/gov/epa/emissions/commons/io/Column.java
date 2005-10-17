package gov.epa.emissions.commons.io;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Column {

    private ColumnFormatter formatter;

    private String name;

    public Column(ColumnFormatter formatter, String name) {
        this.formatter = formatter;
        this.name = name;
    }

    public String format(ResultSet data) throws SQLException {
        return formatter.format(name, data);
    }

    public String name() {
        return name;
    }

}
