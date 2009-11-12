package gov.epa.emissions.commons.data;

import java.io.Serializable;

public class Reference extends LockableImpl implements Serializable {

    private int id;

    private String description;

    private boolean updatedReference;

    public Reference() {
        // no-op
    }

    public Reference(int id, String description) {

        this.id = id;
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isUpdatedReference() {
        return updatedReference;
    }

    public void setUpdatedReference(boolean updatedReference) {
        this.updatedReference = updatedReference;
    }

    @Override
    public String toString() {
        return this.description;
    }

    @Override
    public boolean equals(Object that) {

        boolean retVal = false;
        if (that instanceof Reference) {
            retVal = this.getDescription().equalsIgnoreCase(((Reference) that).description);
        }

        return retVal;
    }

    @Override
    public int hashCode() {
        return this.description.toLowerCase().hashCode();
    }

}
