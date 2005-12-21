package gov.epa.emissions.commons.io;

public class StringFormat {

    private int size;

    public StringFormat(int size) {
        this.size = size;
    }

    public String format(String val) {
        if (val.length() == 0 || val.length() >= size)
            return val;

        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < (size - val.length()); i++)
            buf.append(" ");
        buf.append(val);

        return buf.toString();
    }

}
