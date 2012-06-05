package com.netflix.astyanax.mapping;

@SuppressWarnings({ "UnusedDeclaration", "SimplifiableIfStatement" })
public class FakeKeyspaceBean implements Comparable<FakeKeyspaceBean> {
    @Id("PK")
    private String id;

    @Column("OVERRIDE_BY_TYPE")
    private String type;

    @Column("COUNTRY_OVERRIDE")
    private String country;

    @Column("COUNTRY_STATUS_OVERRIDE")
    private Integer countryStatus;

    @Column("UPDATED_BY")
    private String updatedBy;

    @Column("EXP_TS")
    private Long expirationTS;

    @Column("CREATE_TS")
    private Long createTS;

    @Column("LAST_UPDATE_TS")
    private Long lastUpdateTS;

    @Column("BYTE_ARRAY")
    private byte[] byteArray;

    /**
     * Public empty constructor needed
     */
    public FakeKeyspaceBean() {
    }

    /**
     * Unique identifying id
     * 
     * @return value
     */
    public String getId() {
        return id;
    }

    /**
     * Set unique override id.
     * 
     * @param id
     *            value
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Returns the type of override
     * 
     * @return value
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the override type
     * 
     * @param type
     *            value
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Return the ISO 3166 country code to force on the current request context
     * 
     * @return value
     */
    public String getCountry() {
        return country;
    }

    /**
     * Sets the ISOCountry to force on the current request context
     * 
     * @param country
     *            value
     */
    public void setCountry(String country) {
        this.country = country;
    }

    /**
     * Return country status to force on the overriden country.
     * 
     * @return value
     */
    public Integer getCountryStatus() {
        return countryStatus;
    }

    /**
     * Sets country status to force on the overriden country
     * 
     * @param countryStatus
     *            value
     */
    public void setCountryStatus(Integer countryStatus) {
        this.countryStatus = countryStatus;
    }

    /**
     * Returns the ldap login that made the last update
     * 
     * @return value
     */
    public String getUpdatedBy() {
        return updatedBy;
    }

    /**
     * Sets who made the last update
     * 
     * @param login
     *            value
     */
    public void setUpdatedBy(String login) {
        updatedBy = login;
    }

    /**
     * Returns the creation timestamp in milis since epoch.
     * 
     * @return value
     */
    public Long getCreateTS() {
        return createTS;
    }

    /**
     * Sets the creation timestamp in millis since epoch
     * 
     * @param createTimestamp
     *            value
     */
    public void setCreateTS(Long createTimestamp) {
        createTS = createTimestamp;
    }

    /**
     * Returns the last updated timestamp in millis since epoch
     * 
     * @return value
     */
    public Long getLastUpdateTS() {
        return lastUpdateTS;
    }

    /**
     * Sets the last updated timestamp in millis since epoch
     * 
     * @param updateTimestamp
     *            value
     */
    public void setLastUpdateTS(Long updateTimestamp) {
        lastUpdateTS = updateTimestamp;
    }

    public byte[] getByteArray()
    {
        return byteArray;
    }

    public void setByteArray(byte[] byteArray)
    {
        this.byteArray = byteArray;
    }

    /**
     * Returns the expiration timestamp in millis since epoch
     * 
     * @return value
     */
    public Long getExpirationTS() {
        return expirationTS;
    }

    /**
     * Sets the expiration timestamp in millis since epoch
     * 
     * @param expTS
     *            value
     */
    public void setExpirationTS(Long expTS) {
        expirationTS = expTS;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof FakeKeyspaceBean) {
            return ((FakeKeyspaceBean) o).getId().equals(getId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    /**
     * {@inheritDoc}
     */
    public int compareTo(FakeKeyspaceBean o) {
        if (o == null) {
            return -1;
        } else {
            return getId().compareTo(o.getId());
        }
    }
}