package AR.Utils

import AR.Utils.AssociationRules.Rule
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.immutable.HashSet
import scala.util.control.Breaks


object Recommendation
{
    //关联规则的形式 [前件] => 后件 : 置信度
    //购买了前件中的商品,就推荐后件

    //用于比较函数，参数是关联规则置信度c和后件r,
    //在后续使用中，优先根据置信度降序排序;置信度相同则按r字典序升序排序
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

    /*
    * 根据关联规则和用户购物数据进行推荐
    * rules: 关联规则
    * userData: 用户数据.这里是处理并排序过的(main函数中)
    * sc: Spark Context
    * savePath: 推荐结果储存路径
    */
    def run(rules: RDD[Rule], userData: RDD[Array[Int]], sc: SparkContext, savePath: String): Unit =
    {
        //将关联规则排序
        val sortedRules = rules.sortBy(rule =>
            new RuleToCompare(rule.confidence, rule.consequent(0).toString), ascending = false
        )
        //由于各个slave使用的关联规则是同一份且只读,需要将其broadcast出去
        val assRules = sc.broadcast(sortedRules.collect())
        //用户数据RDD[Array[Int]]
        //将Array改为HashSet,在后面对其使用contains方法更快
        //然后给各个用户数据加上序号,便于完成推荐后再按序输出
        val userDataRdd = userData.map(items => HashSet(items: _*)).zipWithIndex()

        //mapPartitions使用的函数
        //后续使用时,输入的迭代器实际上就是userDataRdd的迭代器
        //输出推荐结果的迭代器.[推荐结果, 用户序号]
        def calculate(iter: Iterator[(HashSet[Int], Long)]): Iterator[(Long, Int)] =
        {
            var res = List[(Long, Int)]()      //推荐结果
            while (iter.hasNext)
            {
                val user = iter.next            //按序取用户
                var recItem = 0    //如果没有推荐的物品，使用0来表示
                val loop = new Breaks
                loop.breakable          //带break的循环
                {
                    //将每条关联规则都尝试一遍,由于关联规则有序且只推荐一项,只要找到的第一条
                    for (i <- assRules.value.indices)
                    {
                        var flag = true     //
                        val end = assRules.value(i).consequent(0)   //规则后件
                        //判断规则是否符合条件:
                        //用户购买的物品不少于这条规则的前件数量,且用户不能已经购买了后件
                        if (user._1.size >= assRules.value(i).antecedent.length && !user._1.contains(end))
                        {
                            //然后对规则前件中的每件商品，都去查看用户是否购买了
                            //如果用户全买了,则flag为true,得到推荐,并退出外部循环
                            //出现没买,这条规则作废,退出内部循环,继续查看下一条规则
                            //如果所有规则都失效,则recItem保留初始值0
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
                //推荐结果 [用户序号, 推荐项]
                res.::=(user._2, recItem)
            }
            res.iterator
        }

        //mapPartitions对每个分区的数据执行calculate,得到推荐项
        //推荐项按key(用户序号)排序
        //排序结果丢弃序号
        //输出到指定路径
        userDataRdd.mapPartitions(calculate).sortByKey().map(t => t._2).saveAsTextFile(savePath)
    }
}