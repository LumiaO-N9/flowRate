package fun.lumia.bean

object scalaClass {

  //杭研B2B
  // B2B Model
  case class B2BModel(timestamp: Long,
                      //                      format_timestamp: String,
                      //                      city: String,
                      server_ip: String,
                      reqdomain: String,
                      filesize: Double,
                      download_filesize: Double,
                      upstream_filesize: Double,
                      //                      reqstarttime: Long,
                      //                      reqendtime: Long,
                      sendtime: Double,
                      //                      res: Double,
                      //                      upres: Double,
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

  class unionOTT(timestamp: Long,
                 server_ip: String,
                 vendor: String,
                 viewtype: String,
                 http_4xx_count: Int,
                 http_5xx_count: Int,
                 http_200_count: Int,
                 http_206_count: Int,
                 http_301_count: Int,
                 http_302_count: Int,
                 http_403_count: Int,
                 http_404_count: Int,
                 http_502_count: Int,
                 http_503_count: Int,
                 upstream_5xx: Int,
                 filesize: Double,
                 download_filesize: Double,
                 tscount: Int,
                 tsbadcount: Int,
                 total: Int,
                 firsttime: Double,
                 upstream_filesize: Double,
                 sendtime: Long
                ) extends Product {
    override def productElement(n: Int): Any = n match {
      case 0 => timestamp
      case 1 => server_ip
      case 2 => vendor
      case 3 => viewtype
      case 4 => http_4xx_count
      case 5 => http_5xx_count
      case 6 => http_200_count
      case 7 => http_206_count
      case 8 => http_301_count
      case 9 => http_302_count
      case 10 => http_403_count
      case 11 => http_404_count
      case 12 => http_502_count
      case 13 => http_503_count
      case 14 => upstream_5xx
      case 15 => filesize
      case 16 => download_filesize
      case 17 => tscount
      case 18 => tsbadcount
      case 19 => total
      case 20 => firsttime
      case 21 => upstream_filesize
      case 22 => sendtime
    }

    override def productArity: Int = 23

    override def canEqual(that: Any): Boolean = that.isInstanceOf[unionOTT]
  }

  // Cpu
  case class CpuPuluo(cpu_time_stamp: Long,
                      cpu_time_stamp2: Long,
                      instance: String,
                      city: String,
                      business: String,
                      group_name: String,
                      server_ip: String,
                      vendor: String,
                      mode: String,
                      cpu: String,
                      value: Double)

}
