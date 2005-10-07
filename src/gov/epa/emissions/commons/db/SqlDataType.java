package gov.epa.emissions.commons.db;

public interface SqlDataType {

    String getType(String name, String genericType, int width);

    String getString(int size);

    String getInt();

    String getLong();

}
