package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.DbColumn;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Column implements DbColumn {

    private String name;

    private String sqlType;

    private ColumnFormatter formatter;

    private int width;

    public Column(String name, String sqlType, ColumnFormatter formatter) {
        this.name = name;
        this.sqlType = sqlType;
        this.formatter = formatter;
        this.width = -999;
    }

    public Column(String name, String sqlType, int width, ColumnFormatter formatter) {
        this(name, sqlType, formatter);
        this.width = width;
    }

    public Column(String name, String sqlType) {
        this(name, sqlType, new NullFormatter());
    }

    public String format(ResultSet data) throws SQLException {
        return formatter.format(name, data);
    }

    public String name() {
        return name;
    }

    public String sqlType() {
        return sqlType;
    }

    public int width() {
        return width;
    }

}
