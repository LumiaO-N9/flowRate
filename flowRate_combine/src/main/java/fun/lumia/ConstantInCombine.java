package fun.lumia;

import fun.lumia.common.Config;

public class ConstantInCombine {


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
    public static String GROUP_ID_CONFIG_TEST = Config.getString("group.id.config.test");
    //AUTO_OFFSET_RESET_CONFIG
    public static String AUTO_OFFSET_RESET_CONFIG = Config.getString("auto.offset.reset.config");
    // hy kafka b2b topic name
    public static String HANG_YAN_KAFKA_TOPIC_NAME = Config.getString("hang.yan.kafka.b2b.topic.name");
    public static String HANG_YAN_VENDOR_NAME = Config.getString("hang.yan.vendor.name");
    public static String HANG_YAN_SIMPLE_NAME = Config.getString("hangyan.vendor.simple.name");
    // zte kafka b2b topic name
    public static String ZTE_KAFKA_B2B_TOPIC_NAME = Config.getString("zte.kafka.topic.b2b.topic.name");
    // fonsview kafka b2b topic name
    public static String FONSVIEW_KAFKA_B2B_TOPIC_NAME = Config.getString("fonsview.kafka.topic.b2b.topic.name");
    public static String COMBINE_CHECKPOINT_PATH_NAME = Config.getString("combine.checkpoint.path.name");
    //kafka ott servers
    public static String KAFKA_BOOTSTRAP_SERVERS_OTT = Config.getString("kafka.bootstrap.servers.ott");
    // huawei ott topic name
    public static String HUAWEI_VENDOR_NAME = Config.getString("huawei.vendor.name");
    public static String HUAWEI_VENDOR_SIMPLE_NAME = Config.getString("huawei.vendor.simple.name");
    public static String HUAWEI_KAFKA_OTT_TOPIC_NAME = Config.getString("huawei.kafka.ott.topic.name");
    // zte ott topic name
    public static String ZTE_VENDOR_NAME = Config.getString("zte.vendor.name");
    public static String ZTE_VENDOR_SIMPLE_NAME = Config.getString("zte.vendor.simple.name");
    public static String ZTE_KAFKA_OTT_TOPIC_NAME = Config.getString("zte.kafka.topic.ott.topic.name");
    public static String ZTE_KAFKA_OTT_TOPIC_NAME_NEW = Config.getString("zte.kafka.topic.ott.topic.name.new");
    // fonsvie ott topic name
    public static String FONSVIEW_VENDOR_NAME = Config.getString("fonsview.vendor.name");
    public static String FONSVIEW_VENDOR_SIMPLE_NAME = Config.getString("fonsview.vendor.simple.name");
    public static String FONSVIEW_KAFKA_OTT_TOPIC_NAME = Config.getString("fonsview.kafka.topic.ott.topic.name");
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

    public static String MYSQL_DATABLE_B2B_KPI = Config.getString("mysql.dbtable.b2b.kpi");
    public static String MYSQL_DATABLE_OTT_KPI = Config.getString("mysql.dbtable.ott.kpi");
    public static String MYSQL_DATABLE_B2B_KPI_DWS = Config.getString("mysql.dbtable.b2b.kpi.dws");
    public static String MYSQL_DATABLE_OTT_KPI_DWS = Config.getString("mysql.dbtable.ott.kpi.dws");
    public static String MYSQL_DATABLE_B2B_KPI_LUMIA = Config.getString("mysql.dbtable.b2b.kpi.lumia");
    public static String MYSQL_DATABLE_B2B_KPI_DWS_LUMIA = Config.getString("mysql.dbtable.b2b.kpi.dws.lumia");
    public static String MYSQL_DATABLE_OTT_KPI_LUMIA = Config.getString("mysql.dbtable.ott.kpi.lumia");
    public static String MYSQL_DATABLE_OTT_KPI_DWS_LUMIA = Config.getString("mysql.dbtable.ott.kpi.dws.lumia");

    public static String MYSQL_USER = Config.getString("mysql.user");
    public static String MYSQL_PASSWORD = Config.getString("mysql.password");


    // CPU Data Configuration
    public static String KAFKA_BOOTSTRAP_SERVERS_CPU = Config.getString("kafka.bootstrap.servers.cpu");
    public static String CPU_KAFKA_TOPIC_NAME = Config.getString("cpu.kafka.topic.name");
    public static String CPU_CHECKPOINT_PATH_NAME = Config.getString("cpu.checkpoint.path.name");
    public static String CPU_ES_INDEX = Config.getString("cpu.es.index");

}
