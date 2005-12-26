package gov.epa.emissions.commons.io;

import java.io.Serializable;
import java.util.Date;

public interface NewDataset extends Serializable {

    public long getDatasetid();

    public void setDatasetid(long datasetid);

    public String getName();

    public void setName(String name);

    void setCreator(String creator);// TODO: use User instead

    String getCreator();

    void setDatasetType(DatasetType datasetType);

    DatasetType getDatasetType();

    void setUnits(String units);

    String getUnits();

    void setRegion(String region);

    String getRegion();

    String getCountry();

    void setCountry(String country);
    
    //TODO: use TemporalResolution
    void setTemporalResolution(String resolution);

    String getTemporalResolution();

    void setStartDateTime(Date time);

    Date getStartDateTime();

    void setStopDateTime(Date time);

    Date getStopDateTime();

    String getDescription();

    void setDescription(String description);

    // Datasets for non-external files will have a list of
    // Internal Source objects. An internal source object will
    // contain details of each table name/table type/ source/ size
    InternalSource[] getInternalSources();

    void setInternalSources(InternalSource[] internalSources);

    void addInternalSource(InternalSource source);

    // Datasets for external files will have a list of
    // External Source objects. An external source object will
    // contain details of each source (sourcename etc)
    ExternalSource[] getExternalSources();

    void setExternalSources(ExternalSource[] externalSources);

    void addExternalSource(ExternalSource source);

}