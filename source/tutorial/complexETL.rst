Complex ETL
************

**二值化处理**
**脏数据清理**

二值化处理是机器学习预处理阶段常用的手段。
比如我们将发生购买行为的用户标记为1，其余标记为0。

用户可能发生多次购买行为，但是我们将把所有大于 1 的次数值设为 1。
如果用户购买了某商品至少一次，那么就认为该用户可能会成为一个回购用户。
二值目标变量，是一个很好的衡量用户购买行为的指标。

.. code-block:: python

   >>> import pandas as pd
   >>> listen_count = pd.read_csv('millionsong/train_triplets.txt.zip', ... header=None, delimiter='\t')
   # 表中包含有形式为“用户-商品-购买次数”的三元组。只包含非零购买次数。 # 因此，要二值化购买次数，只需将整个购买次数列设为1。
   >>> listen_count[2] = 1

.. code-block:: scala

   println("wow")


我们本次构建的ETL链路整体效果如下：

.. image:: ../_static/complexetl.png
  :width: 600
  :alt: Alternative text   


1. `Table Loader` 节点完成数据接入的工作，在该节点的 ``input`` 中选择最初添加好的数据源（``Data Source``）以及源表（``Table``），使用 ``Add operation`` 中 ``keep`` 操作保留需要的字段，这样的操作相当于在UI界面下完成了一次SQL语句的SELECT字段操作。继续 ``Add operation`` 添加一个 ``filter`` 相当于SQL语句的WHERE筛选操作。常见的二维表逻辑操作，都可以在这里通过配置的方式实现。
2. `Aggregate` 节点里 ``Group by columns`` 选择 ``userId`` 和 ``sendTime`` ， ``Add aggregate functions`` 弹窗内配置 ``Select function`` 为 ``count``， ``Select columns`` 选择为 ``All string type columns``。 
3. `Case When` 节点做二值化区分，当`count`大于1时候，清洗数据为 1
4. `SQL` 我们只取出3000条数据。
5. `Entity Identifier` 配置 ``Process`` ，让 ``Entity Type` 设置为`User``，``Column`` 设置为 ``userId``。这样做的含义是让系统知道哪个字段代表着用户ID，原因是不同系统间表意用户ID的字段名可能千奇百怪，我们需要手动的标识出来哪个字段代表用户(User)，哪个字段代表商品(Item)。