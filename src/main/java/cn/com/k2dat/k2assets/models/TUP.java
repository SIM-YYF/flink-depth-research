package cn.com.k2dat.k2assets.models;


import java.util.Objects;

public class TUP implements Comparable<TUP> {
    public Integer id;
    public Long accessTime;
    public String userId;
    public String region;

    public TUP() {
    }

    public TUP(Integer id, Long accessTime, String userId, String region) {
        this.id = id;
        this.accessTime = accessTime;
        this.userId = userId;
        this.region = region;
    }

    @Override
    public String toString() {
        return "UP{" +
                "id=" + id +
                ", accessTime=" + accessTime +
                ", userId='" + userId + '\'' +
                ", region='" + region + '\'' +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TUP)) return false;
        TUP up = (TUP) o;
        return id.equals(up.id) &&
                accessTime.equals(up.accessTime) &&
                userId.equals(up.userId) &&
                region.equals(up.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, accessTime, userId, region);
    }

    /**
     * 基于时间戳进行比较
     * @param o
     * @return
     */
    @Override
    public int compareTo(TUP o) {
        return this.accessTime.compareTo(o.accessTime);
    }
}
