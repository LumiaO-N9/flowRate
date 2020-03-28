package fun.lumia.bean

object scalaClass {

  //杭研B2B
  // B2B Model
  case class B2BModel(timestamp: Long,
                      city: String,
                      server_ip: String,
                      reqdomain: String,
                      filesize: Double,
                      upstream_filesize: Double,
                      reqstarttime: Long,
                      reqendtime: Long,
                      res: Double,
                      upres: Double,
                      firsttime: Double,
                      vendor: String,
                      httpstatus_4xx: Int,
                      httpststus_5xx: Int,
                      upstream_4xx: Int,
                      upstream_5xx: Int,
                      hit_status: Int,
                      count: Int,
                      hit_filesize: Double
                     )

  // 中兴B2B
  //  case class ZTEB2B()

  // 烽火B2B
  //  case class FonsviewB2B()
  case class OttModel(
                       server_ip: String,
                       viewtype: String,
                       vendor: String,
                       //                       serverbyte: Double, // filesize
                       res: Double,
                       starttime: Long,
                       endtime: Long,
                       //                       costtime: Double, // sendtime
                       firsttime: Double,
                       httpstatus_2xx: Int,
                       httpstatus_3xx: Int,
                       httpstatus_4xx: Int,
                       httpstatus_5xx: Int,
                       upstatus_5xx: Int,
                       total: Int
                     )

}
