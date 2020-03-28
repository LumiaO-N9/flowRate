package fun.lumia;

import fun.lumia.common.Config;

public class ComputeConstant {


    //kafka  zookeeper连接地址
    public static String KAFKA_ZOOKEEPER_CONNECT = Config.getString("kafka.zookeeper.connect");

    //kafka servers
    public static String KAFKA_BOOTSTRAP_SERVERS = Config.getString("kafka.bootstrap.servers");
    //spark streaming checkpoint 路径
    public static String SPARK_STREAMING_CHECKPOINT_PATH = Config.getString("spark.streaming.checkpoint.path");
    //spark streaming 落地路径
    public static String SPARK_STREAMING_WRITE_PATH = Config.getString("spark.streaming.write.path");
    public static String SPARK_STREAMING_OTT_WRITE_PATH = Config.getString("spark.streaming.ott.write.path");
    //GROUP_ID_CONFIG
    public static String GROUP_ID_CONFIG = Config.getString("group.id.config");
    //AUTO_OFFSET_RESET_CONFIG
    public static String AUTO_OFFSET_RESET_CONFIG = Config.getString("auto.offset.reset.config");
    // hy kafka b2b topic name
    public static String HANG_YAN_KAFKA_TOPIC_NAME = Config.getString("hang.yan.kafka.b2b.topic.name");
    // zte kafka b2b topic name
    public static String ZTE_KAFKA_B2B_TOPIC_NAME = Config.getString("zte.kafka.topic.b2b.topic.name");
    // fonsview kafka b2b topic name
    public static String FONSVIEW_KAFKA_B2B_TOPIC_NAME = Config.getString("fonsview.kafka.topic.b2b.topic.name");
    // MySQL Config
    public static String MYSQL_URL = Config.getString("mysql.url");
    public static String MYSQL_DRIVER = Config.getString("mysql.driver");
    public static String MYSQL_DBTABLE = Config.getString("mysql.dbtable");
    public static String MYSQL_DBTABLE_OTT = Config.getString("mysql.dbtable.ott");
    public static String MYSQL_DBTABLE_CMDB = Config.getString("mysql.dbtable.cmdb");
    public static String MYSQL_DATABLE_COPY = Config.getString("mysql.dbtable.copy");
    public static String MYSQL_DATABLE_B2B_TOPIC = Config.getString("mysql.dbtable.b2b.topic");
    public static String MYSQL_DATABLE_B2B_TOPIC_TEST = Config.getString("mysql.dbtable.b2b.topic.test");
    public static String MYSQL_DATABLE_DWS = Config.getString("mysql.dbtable.dws");
    public static String MYSQL_USER = Config.getString("mysql.user");
    public static String MYSQL_PASSWORD = Config.getString("mysql.password");
    // 计算区间大小
    public static String COMPUTE_DISTRICT_SECONDS = Config.getString("compute.district.seconds");
    public static String COMPUTE_DISTRICT_MILLISECONDS = Config.getString("compute.district.millisecond");
    public static String COMPUTE_DISTRICT_SECONDS_OTT = Config.getString("compute.district.seconds.ott");
    // 厂商名
    public static String HANGYAN_VENDOR_NAME = Config.getString("hang.yan.vendor.name");
    public static String FONSVIEW_VENDOR_NAME = Config.getString("fonsview.vendor.name");
    public static String HUAWEI_VENDOR_NAME = Config.getString("huawei.vendor.name");
    public static String ZTE_VENDOR_NAME = Config.getString("zte.vendor.name");
    public static String HUAWEI_VENDOR_SIMPLE_NAME = Config.getString("huawei.vendor.simple.name");
    public static String ZTE_VENDOR_SIMPLE_NAME = Config.getString("zte.vendor.simple.name");
    public static String FONSVIEW_VENDOR_SIMPLE_NAME = Config.getString("fonsview.vendor.simple.name");
    public static String HANGYAN_VENDOR_SIMPLE_NAME = Config.getString("hangyan.vendor.simple.name");


}
