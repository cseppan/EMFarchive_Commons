package gov.epa.emissions.commons.db;

public interface SqlTypeMapper {

    String getSqlType(String name, String genericType, int width);

    String getString(int size);

    String getInt();

    String getLong();

}
