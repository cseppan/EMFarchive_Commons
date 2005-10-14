package gov.epa.emissions.commons.io.importer;

public class InternalSource {

    private String source;
    private String table;
    private String type;
    private String[] cols;

    public String getTable() {
        return table;
    }

    public String getType() {
        return type;
    }

    public String[] getCols() {
        return cols;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setCols(String[] cols) {
        this.cols = cols;
    }

}
