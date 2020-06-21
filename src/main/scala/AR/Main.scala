package AR

import AR.Utils.{AssociationRules, Recommendation}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object Main
{
    def main(args: Array[String]): Unit =
    {
        //获取三个参数
        val inputPath = args(0) //输入文件夹路径
        val outputPath = args(1) //输出文件夹路径
        val tempPath = args(2)

        //part 1 生成关联规则
        //配置环境
        val sparkConf = new SparkConf().setAppName("whatever").setMaster("local") //todo local之后要去掉
        val sc = new SparkContext(sparkConf)
        //读取数据 todo 这里文件名需要改回来
        val data = sc.textFile(inputPath + "/cailanzi.txt") //data: RDD[String]
        val transactions = data.map(s => s.trim.split(' ').map(f => f.toInt)) //RDD[Array[Int]]
        val result = new FPGrowth().setMinSupport(0.15).run(transactions) //todo
        result.freqItemsets.persist(StorageLevel.MEMORY_AND_DISK_SER) //持久化存储
        //按要求保存频繁模式结果
        val freqSorted = result.freqItemsets.map(itemset => s"${itemset.items.mkString(" ")}").sortBy(f => f)
        freqSorted.saveAsTextFile("D:\\DevTools\\work\\pattern-2")


        //result.freqItemsets 频繁项集 RDD[FreqItemset[Item]]
        //项集就是购买商品的集合
        //频繁项集是支持度达标的项集

        //生成关联规则
        val assRules = new AssociationRules().run(result.freqItemsets)

        //part 2 推荐
        val userData = sc.textFile("D:\\DevTools\\work\\yonghu.txt").map(items => //todo
            items.trim.split(' ').map(s => s.toInt).sorted
        )
        val savePath = "D:\\DevTools\\work\\rec-1"
        Recommendation.run(assRules, userData, sc, savePath)
    }
}
