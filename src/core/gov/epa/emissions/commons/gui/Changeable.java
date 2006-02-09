package gov.epa.emissions.commons.gui;

public interface Changeable {
    boolean hasChanges();
    
    void observe(ChangeablesList list);
    
    void setChanges(boolean status);
    
    void notifyChanges();
}
