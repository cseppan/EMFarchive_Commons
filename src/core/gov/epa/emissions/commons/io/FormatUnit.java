package gov.epa.emissions.commons.io;


public interface FormatUnit {

    FileFormat fileFormat();

    TableFormat tableFormat();

    boolean isRequired();

    void setInternalSource(InternalSource internalSource);

    InternalSource getInternalSource();
}