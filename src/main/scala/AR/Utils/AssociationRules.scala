package AR.Utils

import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD
import AR.Utils.AssociationRules.Rule

/**
 * 通过FPGrowth算法得出的频繁项集，生成关联规则
 * 根据题目，只生成推荐单个商品的规则
 */
class AssociationRules() extends Serializable
{
    /**
     * 生成关联规则
     * @param freqItemsets FPGrowth算法的结果
     * @return 规则
     */
    def run(freqItemsets: RDD[FreqItemset[Int]]): RDD[Rule] =
    {
        //每个candidate是(antecedent.toSeq, (consequent.toSeq, itemset.freq))
        //rule X => Y:  ([X], ([Y], freq([X] + [Y])))
        val candidates = freqItemsets.flatMap
        {
            itemset =>
                val items = itemset.items       //items: Array[Int]
                items.flatMap
                {
                    item =>                     //item: Int
                        //分别取某个商品做后件，其他商品做前件,要求前件不为空
                        //([A], [B]) = partition(p): 满足p的放A中,不满足的放B中
                        items.partition(_ == item) match
                        {
                            //该商品作为后件能与剩余商品构成规则
                            case (consequent, antecedent) if !antecedent.isEmpty =>
                                Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
                            case _ => None
                        }
                }
        }

        // Join to get (X, ((Y, freq(X union Y)), freq(X))), generate rules
        val t = freqItemsets.map(x => (x.items.toSeq, x.freq))
        candidates.join(t)
                //.filter(f => f._2._1._2.toDouble / f._2._2.toDouble >= minConfidence)
                .map
                {
                    case (antecedent, ((consequent, freqUnion), freqAntecedent)) =>
                    new Rule(antecedent.toArray.sorted, consequent.toArray, freqUnion.toDouble / freqAntecedent)
                }
    }
}

object AssociationRules
{
    /**
     * Like fpm.AssociatedRules.Rule
     * @param antecedent 规则的前件
     * @param consequent 规则的后件
     * @param confidence 置信度
     */
    class Rule(val antecedent: Array[Int],
               val consequent: Array[Int],
               val confidence: Double) extends Serializable
    { }
}

