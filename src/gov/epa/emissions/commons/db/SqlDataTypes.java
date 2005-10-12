package gov.epa.emissions.commons.db;

public interface SqlDataTypes {

    String type(String name, String genericType, int width);

    String stringType(int size);

    String intType();

    String longType();

    String realType();

    String smallInt();

    String charType();

}
