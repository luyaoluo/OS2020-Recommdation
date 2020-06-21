package AR.Utils

import AR.Utils.AssociationRules.Rule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.immutable.HashSet
import scala.util.control.Breaks


object Recommendation
{
    class RuleToCompare(c: Double, r: String) extends Comparable[RuleToCompare] with Serializable
    {
        var confidence: Double = c
        var rec: String = r
        override def compareTo(o: RuleToCompare): Int =
        {
            if (this.confidence == o.confidence)
            {
                o.rec compare this.rec
            }
            else
            {
                this.confidence compare o.confidence
            }
        }
    }

    def run(rules: RDD[Rule], userData: RDD[Array[Int]], sc: SparkContext, savePath: String): Unit =
    {
        //根据confidence降序排列
        val sortedRules = rules.sortBy(rule =>
            new RuleToCompare(rule.confidence, rule.consequent(0).toString), ascending = false
        )

        val assRules = sc.broadcast(sortedRules.collect())
        //分片
        val userDataRdd = userData.map(items => HashSet(items: _*)).zipWithIndex()

        def calculate(iter: Iterator[(HashSet[Int], Long)]): Iterator[(Long, Int)] =
        {
            var res = List[(Long, Int)]()
            while (iter.hasNext)
            {
                val user = iter.next
                var recItem = 0    //如果没有推荐的物品，使用0来表示
                val loop = new Breaks
                loop.breakable
                {
                    for (i <- assRules.value.indices)
                    {
                        var flag = true
                        val end = assRules.value(i).consequent(0)
                        //判断规则是否符合条件
                        if (user._1.size >= assRules.value(i).antecedent.length && !user._1.contains(end))
                        {
                            val iterate = assRules.value(i).antecedent.iterator
                            val loopRule = new Breaks
                            loopRule.breakable
                            {
                                while (iterate.hasNext)
                                {
                                    if (!user._1.contains(iterate.next()))
                                    {
                                        flag = false
                                        loopRule.break()
                                    }
                                }
                            }
                            if (flag)
                            {
                                recItem = end
                                loop.break()
                            }
                        }
                    }
                }
                res.::=(user._2, recItem)
            }
            res.iterator
        }

        userDataRdd.mapPartitions(calculate).map(t => t._2).saveAsTextFile(savePath)
    }
}