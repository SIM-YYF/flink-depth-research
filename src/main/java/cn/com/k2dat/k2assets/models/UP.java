package cn.com.k2dat.k2assets.models;


import java.util.Objects;

public class UP {
    public Integer id;
    public String accessTime;
    public String userId;
    public String region;

    public UP() {
    }

    public UP(Integer id, String accessTime, String userId, String region) {
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
        if (!(o instanceof UP)) return false;
        UP up = (UP) o;
        return id.equals(up.id) &&
                accessTime.equals(up.accessTime) &&
                userId.equals(up.userId) &&
                region.equals(up.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, accessTime, userId, region);
    }
}
