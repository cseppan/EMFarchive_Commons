package gov.epa.emissions.commons.io;

import java.io.Serializable;

public class Table implements Serializable {
    private String tableName;

    private String tableType;

    public Table(String name, String type) {
        this.tableName = name;
        this.tableType = type;
    }

    public Table() {// Needed for Axis serialization i.e. transport-over-wire
    }

    public String getName() {
        return tableName;
    }

    public String getType() {
        return tableType;
    }

    public void setName(String tableName) {
        this.tableName = tableName;
    }

    public void setType(String tableType) {
        this.tableType = tableType;
    }

}
