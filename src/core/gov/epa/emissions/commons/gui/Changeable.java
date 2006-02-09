package gov.epa.emissions.commons.gui;

public interface Changeable {
    boolean hasChanges();
    
    void observe(ChangeablesList list);
    
    void clear();
}
