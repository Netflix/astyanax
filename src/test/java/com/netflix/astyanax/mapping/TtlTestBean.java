package com.netflix.astyanax.mapping;

public class TtlTestBean implements Comparable<TtlTestBean> {
    @Id("PK")
    private String id;

    @Column("DEFAULT_TTL")
    private String defaultTtl;

    @Column(value = "ZERO_TTL", ttl = 0)
    private String zeroTtl;

    @Column(value = "SIXTY_TTL", ttl = 60)
    private String sixtyTtl;

    /**
     * Public empty constructor needed
     */
    public TtlTestBean() {
    }

    /**
     * Unique identifying id
     * 
     * @return value
     */
    public String getId() {
        return id;
    }

    public String getDefaultTtl() {
        return defaultTtl;
    }

    public void setDefaultTtl(String defaultTtl) {
        this.defaultTtl = defaultTtl;
    }

    public String getZeroTtl() {
        return zeroTtl;
    }

    public void setZeroTtl(String zeroTtl) {
        this.zeroTtl = zeroTtl;
    }

    public String getSixtyTtl() {
        return sixtyTtl;
    }

    public void setSixtyTtl(String sixtyTtl) {
        this.sixtyTtl = sixtyTtl;
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

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof TtlTestBean) {
            return ((TtlTestBean) o).getId().equals(getId());
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
    public int compareTo(TtlTestBean o) {
        if (o == null) {
            return -1;
        } else {
            return getId().compareTo(o.getId());
        }
    }
}