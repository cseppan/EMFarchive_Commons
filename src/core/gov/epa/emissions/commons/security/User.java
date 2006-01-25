package gov.epa.emissions.commons.security;

import gov.epa.emissions.commons.CommonsException;
import gov.epa.emissions.commons.io.Lockable;
import gov.epa.emissions.commons.io.Mutex;

import java.io.Serializable;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * The User value object encapsulates all of the EMF user data The User object is serialized between the server and
 * client using Apache Axis Web Services (SOAP/HTTP and XML)
 * 
 */
public class User implements Serializable, Lockable {

    // State variables for the User bean
    private long id;

    private String fullname;

    private String affiliation;

    private String phone;

    private String email;

    private String username;

    private String encryptedPassword;

    private boolean inAdminGroup = false;

    private boolean acctDisabled = false;

    private PasswordGenerator passwordGen;

    private Mutex lock;

    public User() {// needed for serialization
        this.passwordGen = new PasswordGenerator();
        lock = new Mutex();
    }

    public User(String name, String affiliation, String phone, String email, String username, String password,
            boolean beAdmin, boolean disabled) throws UserException {
        this();

        setFullName(name);
        setAffiliation(affiliation);
        setPhone(phone);
        setEmail(email);
        setUsername(username);
        setPassword(password);

        this.inAdminGroup = beAdmin;
        this.acctDisabled = disabled;
    }

    public boolean equals(Object other) {
        if (!(other instanceof User))
            return false;

        User otherUser = (User) other;
        return this.username.equals(otherUser.username);
    }

    public int hashCode() {
        return username != null ? username.hashCode() : 0;
    }

    public boolean isAcctDisabled() {
        return acctDisabled;
    }

    public void setAcctDisabled(boolean acctDisabled) {
        this.acctDisabled = acctDisabled;
    }

    public String getAffiliation() {
        return affiliation;
    }

    public void setAffiliation(String affiliation) throws UserException {
        if (affiliation == null)
            throw new UserException("Affiliation should be specified");

        if (affiliation.length() < 3) {
            throw new UserException("Affiliation should have 2 or more characters");
        }

        this.affiliation = affiliation;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) throws UserException {
        if (email == null)
            throw new UserException("Email should be specified");

        if (!Pattern.matches("^(_?+\\w+(.\\w+)*)(\\w)*@(\\w)+.(\\w)+(.\\w+)*", email))
            throw new UserException("Email should have the format xx@yy.zz");

        this.email = email;
    }

    public String getFullName() {
        return fullname;
    }

    public void setFullName(String name) throws UserException {
        if (name == null || name.length() == 0)
            throw new UserException("Name should be specified");
        this.fullname = name;
    }

    public boolean isInAdminGroup() {
        return inAdminGroup;
    }

    public void setInAdminGroup(boolean inAdminGroup) {
        this.inAdminGroup = inAdminGroup;
    }

    public void setPassword(String password) throws UserException {
        if (password == null)
            throw new UserException("Password should be specified");

        if (password.length() < 8) {
            throw new UserException("Password should have at least 8 characters");
        }

        if (!Pattern.matches("^([a-zA-Z]+)(\\d+)(\\w)*", password)) {
            throw new UserException("One or more characters of password should be a number");
        }

        if (password.equals(username)) {
            throw new UserException("Username should be different from Password");
        }

        try {
            this.encryptedPassword = passwordGen.encrypt(password);
        } catch (CommonsException e) {
            throw new UserException("Error encrypting password");
        }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) throws UserException {
        if (username == null) {
            throw new UserException("Username should be specified");
        }
        if (username.length() < 3) {
            throw new UserException("Username should have at least 3 characters");
        }

        verifyUsernamePasswordDontMatch(username);

        this.username = username;
    }

    private void verifyUsernamePasswordDontMatch(String username) throws UserException {
        if (encryptedPassword == null)
            return;

        String encryptedUsername = null;
        try {
            encryptedUsername = passwordGen.encrypt(username);
        } catch (CommonsException e) {
            throw new UserException("failed on verification of username with password", e.getMessage(), e);
        }
        if (encryptedPassword.equals(encryptedUsername))
            throw new UserException("Username should be different from Password");
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) throws UserException {
        if (phone == null || phone.length() == 0)
            throw new UserException("Phone should be specified");

        this.phone = phone;
    }

    public void confirmPassword(String confirmPassword) throws UserException {
        String encryptConfirmPwd = null;

        try {
            encryptConfirmPwd = passwordGen.encrypt(confirmPassword);
        } catch (CommonsException e) {
            throw new UserException("Error encrypting password");
        }
        if (!encryptedPassword.equals(encryptConfirmPwd)) {
            throw new UserException("Confirm Password should match Password");
        }

    }

    public String getEncryptedPassword() {
        return encryptedPassword;
    }

    public void setEncryptedPassword(String encryptedPassword) {
        this.encryptedPassword = encryptedPassword;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getLockDate() {
        return lock.getLockDate();
    }

    public void setLockDate(Date lockDate) {
        lock.setLockDate(lockDate);
    }

    public boolean isLocked(String owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked(User owner) {
        return lock.isLocked(owner);
    }

    public boolean isLocked() {
        return lock.isLocked();
    }

    public String getLockOwner() {
        return lock.getLockOwner();
    }

    public void setLockOwner(String username) {
        lock.setLockOwner(username);
    }

}
