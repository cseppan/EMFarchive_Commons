package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.DbColumn;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Column implements DbColumn {

    private String name;

    private String sqlType;

    private ColumnFormatter formatter;

    private int width;

    private String constraints;

    public Column(String name, String sqlType, int width, ColumnFormatter formatter, String constraints) {
        this.name = name;
        this.sqlType = sqlType;
        this.formatter = formatter;
        this.width = width;
        this.constraints = constraints;
    }

    public Column(String name, String sqlType, int width, ColumnFormatter formatter) {
        this(name, sqlType, width, formatter, null);
    }

    public Column(String name, String sqlType, ColumnFormatter formatter) {
        this(name, sqlType, -1, formatter);
    }

    public Column(String name, String sqlType) {
        this(name, sqlType, new NullFormatter());
    }

    public Column(String name, String sqlType, ColumnFormatter formatter, String constraints) {
        this(name, sqlType, -1, formatter, constraints);
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

    public String constraints() {
        return constraints;
    }

    public boolean hasConstraints() {
        return constraints != null;
    }

}
