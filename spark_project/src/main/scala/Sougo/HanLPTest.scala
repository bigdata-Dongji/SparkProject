package Sougo


import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import com.hankcs.hanlp.tokenizer.StandardTokenizer


object HanLPTest {
  def main(args: Array[String]): Unit = {
    // 入门Demo
    val terms: util.List[Term] = HanLP.segment("杰克奥特曼全集视频")
    //导包，将java集合类对象转换为scala集合类对象
    import scala.collection.JavaConverters._
    println(terms)
    println(terms.asScala.map(_.word.trim))
    // 标准分词
    val terms1 = StandardTokenizer.segment("放假++端午++重阳")
    println(terms1)

    println(terms1.asScala.map(_.word.replaceAll("\\s+", "")))
    val words: Array[String] =
      """00:00:00 2982199073774412 [360安全卫
士] 8 3 download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html"""
        .split("\\s+")
    words.foreach(println)
    println(words(2).replaceAll("\\[|\\]", ""))
  }
}
