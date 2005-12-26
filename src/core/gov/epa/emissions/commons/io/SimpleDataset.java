package gov.epa.emissions.commons.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleDataset implements Dataset {
    private long datasetid;

    private String name;

    private int year;

    private String description;

    private String datasetTypeName;

    private String region;

    private String country;

    private String units;

    private String creator;

    private String temporalResolution;

    private Date startDateTime;

    private Date endDateTime;

    private List datasources;

    private List internalSources;

    private List externalSources;

    private List tables;

    private DatasetType datasetType;

    private InternalSource summarySource;

    /**
     * No argument constructor needed for hibernate bean mapping
     */
    public SimpleDataset() {
        tables = new ArrayList();
        internalSources = new ArrayList();
        externalSources = new ArrayList();

    }

    public String getDatasetTypeName() {
        return datasetTypeName;
    }

    public void setUnits(String units) {
        this.units = units;
    }

    public void setTemporalResolution(String temporalResolution) {
        this.temporalResolution = temporalResolution;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setStartDateTime(Date time) {
        this.startDateTime = time;
    }

    public void setStopDateTime(Date time) {
        this.endDateTime = time;
    }

    public String getRegion() {
        return region;
    }

    public int getYear() {
        return year;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Table getTable(String tableType) {
        for (Iterator iter = tables.iterator(); iter.hasNext();) {
            Table element = (Table) iter.next();
            if (element.getType().equals(tableType))
                return element;
        }

        return null;
    }

    // TODO: return a list. Also, change the Hibernate mapping
    public Map getTablesMap() {
        Map tablesMap = new HashMap();

        for (Iterator iter = tables.iterator(); iter.hasNext();) {
            Table element = (Table) iter.next();
            tablesMap.put(element.getType(), element.getName());
        }

        return tablesMap;
    }

    public void setDatasetTypeName(String datasetType) {
        this.datasetTypeName = datasetType;
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getUnits() {
        return units;
    }

    public void setTablesMap(Map tablesMap) {
        tables.clear();

        for (Iterator iter = tablesMap.keySet().iterator(); iter.hasNext();) {
            String tableType = (String) iter.next();
            tables.add(new Table((String) tablesMap.get(tableType), tableType));
        }
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreator() {
        return creator;
    }

    public String getTemporalResolution() {
        return temporalResolution;
    }

    public Date getStartDateTime() {
        return startDateTime;
    }

    public Date getStopDateTime() {
        return endDateTime;
    }

    public List getDatasources() {
        return datasources;
    }

    public void setDatasources(List datasources) {
        this.datasources = datasources;
    }

    public long getDatasetid() {
        return datasetid;
    }

    public void setDatasetid(long datasetid) {
        this.datasetid = datasetid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Table[] getTables() {
        return (Table[]) tables.toArray(new Table[0]);
    }

    public boolean equals(Object other) {
        if (other == null || !(other instanceof Dataset)) {
            return false;
        }

        Dataset otherDataset = (Dataset) other;

        return (name.equals(otherDataset.getName()));
    }

    public void setDatasetType(DatasetType datasetType) {
        this.datasetType = datasetType;
    }

    public DatasetType getDatasetType() {
        return datasetType;
    }

    public InternalSource[] getInternalSources() {
        return (InternalSource[]) this.internalSources.toArray(new InternalSource[0]);
    }

    public void setInternalSources(InternalSource[] internalSources) {
        this.internalSources.clear();
        this.internalSources.addAll(Arrays.asList(internalSources));
    }

    public void addInternalSource(InternalSource source) {
        this.internalSources.add(source);
    }

    public ExternalSource[] getExternalSources() {
        return (ExternalSource[]) this.externalSources.toArray(new ExternalSource[0]);
    }

    public void setExternalSources(ExternalSource[] externalSources) {
        this.externalSources.clear();
        this.externalSources.addAll(Arrays.asList(externalSources));
    }

    public void addExternalSource(ExternalSource source) {
        this.externalSources.add(source);
    }

    public void setSummarySource(InternalSource summarySource) {
        this.summarySource = summarySource;
        
    }

    public InternalSource getSummarySource() {
        return summarySource;
    }

}