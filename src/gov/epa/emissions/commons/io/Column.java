package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.DbColumn;

import java.sql.ResultSet;
import java.sql.SQLException;

public class Column implements DbColumn {

    private String name;

    private String sqlType;

    private ColumnFormatter formatter;

    public Column(String sqlType, ColumnFormatter formatter, String name) {
        this.name = name;
        this.sqlType = sqlType;
        this.formatter = formatter;
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

}
