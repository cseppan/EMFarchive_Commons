package gov.epa.emissions.commons.db;

public interface SqlDataTypes {

    String getType(String name, String genericType, int width);

    String getString(int size);

    String getInt();

    String getLong();

    String getReal();

    String smallInt();

}
