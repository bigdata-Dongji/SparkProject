package Sougo

/**
 * 用户搜索点击网页记录Record
 *  queryTime 访问时间，格式为：HH:mm:ss
 *  userId 用户ID
 *  queryWords 查询词
 *  resultRank 该URL在返回结果中的排名
 *  clickRank 用户点击的顺序号
 *  clickUrl 用户点击的URL
 */
case class SogouRecord(
                        queryTime: String,
                        userId: String,
                        queryWords: String,
                        resultRank: Int,
                        clickRank: Int,
                        clickUrl: String
                      )
