package taxi

import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}
/**
 * 测试使用ip2Region工具库解析IP地址为省份和城市
 */
object ConvertIpTest {
  def main(args: Array[String]): Unit = {
    // a. 创建DbSearch对象，传递字典文件
    val dbSearcher = new DbSearcher(new DbConfig(), "datas/advertising/ip2region.db")
    // b. 依据IP地址解析
    val dataBlock: DataBlock = dbSearcher.btreeSearch("180.76.76.76")
    // 中国|0|北京|北京市|百度
    val region: String = dataBlock.getRegion
    println(region)
    // c. 分割字符串，获取省份和城市
    val Array(_, _, province, city, _) = region.split("\\|")
    println(s"省份 = $province, 城市 = $city")
  }
}