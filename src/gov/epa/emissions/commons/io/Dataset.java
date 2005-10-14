/*
 * Creation on Aug 29, 2005
 * Eclipse Project Name: EMF
 * File Name: Dataset.java
 * Author: Conrad F. D'Cruz
 */
/**
 * 
 */

package gov.epa.emissions.commons.io;


import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Dataset extends Serializable {
    // unique id needed for hibernate persistence
    public long getDatasetid();

    public void setDatasetid(long datasetid);

    public String getName();

    public void setName(String name);

    // bean-style properties
    void setCreator(String creator);// TODO: use User instead

    String getCreator();

    String getDatasetTypeName();

    void setDatasetType(DatasetType datasetType);

    DatasetType getDatasetType();

    void setUnits(String units);

    String getUnits();

    void setRegion(String region);

    String getRegion();

    void setYear(int year);

    int getYear();

    String getCountry();

    void setCountry(String country);

    //FIXME: use the TemporalResolution instead
    void setTemporalResolution(String name);

    String getTemporalResolution();

    void setStartDateTime(Date time);

    Date getStartDateTime();

    void setStopDateTime(Date time);

    Date getStopDateTime();

    String getDescription();

    void setDescription(String description);

    Map getTablesMap();

    // FIXME: never used ?
    void setTablesMap(Map datatables);

    void setDatasources(List datasources);

    List getDatasources();

    // Datasets for non-external files will have a list of 
    // Internal Source objects.  An internal source object will
    // contain details of each table name/table type/ source/ size
    List getInternalSources();
    void setInternalSources(List internalSources);
    
    // convenience methods
    Table getTable(String tableType);

    void addTable(Table importedTable);

    public Table[] getTables();

    void addInternalSource(InternalSource source);
}
