package gov.epa.emissions.commons.security;

import gov.epa.emissions.commons.CommonsException;

public class UserException extends CommonsException {

    public UserException(String description, String details, Throwable cause) {
        super(description, details, cause);
    }

    public UserException(String description, String details) {
        super(description, details);
    }

    public UserException(String message) {
        super(message);
    }

}
