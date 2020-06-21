# OS2020-Recommdation
Recommdation Algorithm with Rules Association for USTC 2020 OS(with spark,scala)

### 项目相关

- 在idea上用sbt构建的scala项目

- 核心代码在package AR中，AR/Utils中有两个工具类分别为

  - AssocaitionRules，从频繁模式中提取关联规/
  - Recommedation，利用关联规则为用户进行推荐

- 在Main.scala中调用了两个工具类，同时调用了

  ```
  org.apache.spark.mllib.fpm.FPGrowth
  ```

  spark自带的FPGrowth算法*，用以提取频繁模式

- **注意代码中的文件路径**

- 数据集/测试样本中，trans_10w就是购物篮数据集，test_2w就是用户数据集

  pattern_1w是频繁模式，result是结果

---

### TO DO(Up to 6.21)

#### 代码部分

- 优化一下每个函数的参数（为了方便起见函数的传参部分写的比较乱）以及文件路径的表示，按照ppt所述，路径不能直接写死在程序里。
- 结果还有点小bug
  - 提取的频繁模式与示例相比多很多，不清楚是它给的示例有问题还是我们算的有问题。
  - 最后的推荐结果有少部分与示例不同，原因尚不明确
- spark没有设置过partitionNum（不知道需不需要，这样有可能并不是真正的并行），还涉及到性能调优的问题（不过缺少数据集不用特别关心这个）。。
- 需要将程序打包成jar包(PPT所述)，最后输出也有要求

#### 文档部分

![1592755638(1).jpg](http://ww1.sinaimg.cn/large/006y8jFply1gg0d0on8tuj30kk062wey.jpg)

详情参照ppt。

