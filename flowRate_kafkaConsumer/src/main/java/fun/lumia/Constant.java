package fun.lumia;

import fun.lumia.common.Config;

public class Constant {


    //kafka  zookeeper连接地址
    public static String KAFKA_ZOOKEEPER_CONNECT = Config.getString("kafka.zookeeper.connect");

    //kafka servers
    public static String KAFKA_BOOTSTRAP_SERVERS = Config.getString("kafka.bootstrap.servers");
    //spark streaming checkpoint 路径
    public static String SPARK_STREAMING_CHECKPOINT_PATH = Config.getString("spark.streaming.checkpoint.path");
    public static String SPARK_STREAMING_CHECKPOINT_OTT_PATH = Config.getString("spark.streaming.checkpoint.ott.path");
    //spark streaming 落地路径
    public static String SPARK_STREAMING_WRITE_PATH = Config.getString("spark.streaming.write.path");
    //spark streaming 落地路径
    public static String SPARK_STREAMING_OTT_WRITE_PATH = Config.getString("spark.streaming.ott.write.path");
    //GROUP_ID_CONFIG
    public static String GROUP_ID_CONFIG = Config.getString("group.id.config");
    //AUTO_OFFSET_RESET_CONFIG
    public static String AUTO_OFFSET_RESET_CONFIG = Config.getString("auto.offset.reset.config");
    // hy kafka b2b topic name
    public static String HANG_YAN_KAFKA_TOPIC_NAME = Config.getString("hang.yan.kafka.b2b.topic.name");
    public static String HANG_YAN_VENDOR_NAME=Config.getString("hang.yan.vendor.name");
    public static String HANG_YAN_SIMPLE_NAME=Config.getString("hang.yan.vendor.simple.name");
    // zte kafka b2b topic name
    public static String ZTE_KAFKA_B2B_TOPIC_NAME = Config.getString("zte.kafka.topic.b2b.topic.name");
    // fonsview kafka b2b topic name
    public static String FONSVIEW_KAFKA_B2B_TOPIC_NAME = Config.getString("fonsview.kafka.topic.b2b.topic.name");
    //kafka ott servers
    public static String KAFKA_BOOTSTRAP_SERVERS_OTT = Config.getString("kafka.bootstrap.servers.ott");
    // huawei ott topic name
    public static String HUAWEI_VENDOR_NAME=Config.getString("huawei.vendor.name");
    public static String HUAWEI_VENDOR_SIMPLE_NAME=Config.getString("huawei.vendor.simple.name");
    public static String HUAWEI_KAFKA_OTT_TOPIC_NAME = Config.getString("huawei.kafka.ott.topic.name");
    // zte ott topic name
    public static String ZTE_VENDOR_NAME=Config.getString("zte.vendor.name");
    public static String ZTE_VENDOR_SIMPLE_NAME=Config.getString("zte.vendor.simple.name");
    public static String ZTE_KAFKA_OTT_TOPIC_NAME = Config.getString("zte.kafka.topic.ott.topic.name");
    // fonsvie ott topic name
    public static String FONSVIEW_VENDOR_NAME =Config.getString("fonsview.vendor.name");
    public static String FONSVIEW_VENDOR_SIMPLE_NAME=Config.getString("fonsview.vendor.simple.name");
    public static String FONSVIEW_KAFKA_OTT_HDP_TOPIC_NAME = Config.getString("fonsview.kafka.topic.ott.hdp.topic.name");
    public static String FONSVIEW_KAFKA_OTT_HLS_TOPIC_NAME = Config.getString("fonsview.kafka.topic.ott.hls.topic.name");
    public static String FONSVIEW_KAFKA_OTT_ERROR_TOPIC_NAME = Config.getString("fonsview.kafka.topic.ott.error.topic.name");

}
