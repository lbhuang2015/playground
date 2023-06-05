# 1. Accelerate maven

- Skip testing code

  ```shell
  -Dmaven.test.skip=true；
  ```

- compile by multiple threads

  ```shell
   -Dmaven.compile.fork=true
  ```

- For 3.x or later

Use parameter `-T 1C`

- Example：

```shell
mvn clean package -T 1C -Dmaven.test.skip=true -Dmaven.compile.fork=true
```

- remove Lombok

  if possible remove Lombok

- Others

```shell
MAVEN_OPTS= -XX:+TieredCompilation -XX:TieredStopAtLevel=1
mvn -T 1C install -pl $moduleName -am --offline
#-pl $moduleName -am
#-pl - makes Maven build only specified modules and not the whole project.
#-am - find module's dependencies and built them.
#offline build (using local repo)
```

# 一、processDRCNonSecEodcFeeds

## 1. 取出并过滤配置ValuationMeasureDataset

### (1) 取出过滤

- 从sparkContext获取VALUATION_MEASURE_DATA

- 过滤出measurename值包含在如下集合中的记录：

  （Notional Amount,...,Mark to Market,...,Dirty Price - Bid)

- 通过spark.selectExpr选出如下列生成ValuationMeaseDF数据集

**Dataset< Row > valuationMeaseDF** 是实际的头寸估值结果，包含如下字段：

***"EodPositionId","AssetConstituentRecordid","MeasureName","MeasureLocalCurrencyCode","MeasureReportingCurrencyCode","MeasureLocalValue","MeasureReportingValue","MeasureValueSourceSystemId"***

### (2) 加入RiskWatch标记

针对valuationMeaseDF，通过构造新列根据MeasureValueSourceSystemId的值，将measurename值替换指定值

**主要目的是针对SourceSystem是RisktWatch的情况，在measurename中加入RiskWatch标记**

如果：

- MeasureValueSourceSystemId== RISKWATCH_SOURCE_SYSTEM_ID  &&   measuename == Mark to Market Amount

    则 measuename = "Mark to Market Amount RiskWatch"

- MeasureValueSourceSystemId== RISKWATCH_SOURCE_SYSTEM_ID  &&   measuename == Underlying Mark to Market Amount

    则 measuename = "Underlying Mark to Market Amount RiskWatch"

## 2. 分别处理valuation WithAsset和WithoutAsset

### (1)设置别名映射map

Map<String, String> AliasMap={

"Notional amount":"notional"

....

""Dirty Price -Bid":"dirtypricebid"

}

### (2)分别处理

**Dataset< Row > valuationWithAsset**

   过滤条件AssetConstituentRecordId有值：

​              AssetConstituentRecordId != ' ' && AssetConstituentRecordId !=null && AssetConstituentRecordId != 'NA' 

```scala
vwaDF.groupBy("EodPositionId","AssetConstituentRecordid").pivot("MeasureName").agg(
  first('MeasureLocalCurrencyCode').alias('MeasureLocalCurrencyCode'),
  first('MeasureReportingCurrencyCode').alias('MeasureReportingCurrencyCode'),
  first('MeasureLocalValue').alias('MeasureLocalValue'),
  first('MeasureReportingValue').alias('MeasureReportingValue'),  
)
```

把新的Dataframe的新列列名按别名映射map进行处理和补全

**Dataset< Row > valuationWithoutAsset**

逻辑和withAsset完全一致

## 3. 处理Position Decorator

Fungible Decorator  FNPD

Fungible Decorator通常用于表示头寸的可替代性特征，具体取决于具体的投资产品和标的资产

Interest Rate Decorator IRPD

Interest Rate Decorator通常用于描述头寸与利率之间的关联，并提供了更详细的信息以解释头寸的特定特征。

Equity Decorator EQPD

Equity Decorator用于描述头寸与股权市场相关的特定属性，并提供了更详细的信息以解释头寸的特征

Credit Default Swap Decorator CDPD

CDS Decorator用于描述头寸与信用违约掉期市场相关的特定属性，并提供了更详细的信息以解释头寸的特征。

### (1) 合并所有PD的列名

通过rdlSparkContext取得FNPD，IRPD，EQPD，CDPD四个dataset的所有列名，合并到一个字符串数组中String[] **reqPDColumns**

### (2) 分类原始的标准positionNonSecData数据集

分如下类别

- CDS产品

positionCDSDF = posNonSecDF.filter(CDS)

- Equity Swap产品

positionEQSwapDF=posNonSecDF.filter(EqSwap)

- 其他的positiondecoratortype in (FNPD,IRPD,EQPD,CDPD)的产品

othersWithPD

- 其他的不属于前三类的

otherPositions

### (3) 生成原始positionData+所有PD的列名数组

posPDAssetColumns

### (4) 处理othersWithPD, joinWithPositionDecorators

```java
othersWithPD = joinWithPositionDecorators(reqPDColumns,
                                          othersWithPD,
                                          CDPD_DATA,
                                          IRPD_DATA,
                                          EQPD_DATA,
                                          FNPD_DATA)
 Dataset<Row> joinWithPositionDecorators(String[] reqColumns,
                                         Dataset<Row> positionDf, 
                                         Dataset<Row>...pdDataFrameArr)
//如果第三个参数没有数据可以join,就直接在第二个参数dataset中加上第一个参数中的所有列，但列值都为空
//如果第三个参数有数据，先把这个参数中所有的数据集进行union,其结果包含第一个reqPDColumns所有列，然后把这个结果和第二个参数进行join,join的key是positiondecoratorid和positiondecoratortype
```

### (5) 处理otherPositions, joinWithPositionDecorators

```java
otherPositions = joinWithPositionDecorators(reqPDColumns,
                                          otherPositions)
 Dataset<Row> joinWithPositionDecorators(String[] reqColumns,
                                         Dataset<Row> positionDf, 
                                         Dataset<Row>...pdDataFrameArr)
//这里第三个参数没有数据可以join,就直接在第二个参数dataset中加上第一个参数中的所有列，但列值都为空
```

### (6) 合并othersWithPD到otherPositions

otherPositions

### (7) 处理positionCDSDF, joinWithPositionDecorators

```java
positionCDSDF = joinWithPositionDecorators(reqPDColumns,
                                          positionCDSDF,
                                          CDPD_DATA)
```

### (8) 处理positionEqSwapDF, joinWithPositionDecorators

```java
positionEqSwapDF = joinWithPositionDecorators(reqPDColumns,
                                          positionEqSwapDF,
                                          EQPD_DATA)
```

现在有三类已经Join过position Decorator的数据集

- otherPositions

- positionCDSDF

- positionEqSwapDF

## 4. 在包含PD数据的positionCDSDF中增加valuationMeasurement

```java
drcNonSecCDSJoinDF = processDRCNonSecCDSRecords(rdlSparContext.getSparkSession(),
                                               positionCDSDF,
                                               valuationWithoutAssest,
                                               valuationWithAssest,
                                               logStr,
                                               timeTracker
                                               ASSET_CONSTITUENT_DATA)
/**
processDRCNonSecCDSRecords函数的功能：
*/
```

### (1) 针对CDS single name的情况

不需要join AssetConstituent，只需要添加空数据列，再和valuationWithoutAsset进行join即可

- 通过positionCDSDF.filter(POSITION_CONSTITUENT_FLAG_N)过滤出对应的数据集

- 添加assetConstituentDF.columns的所有列(列值为空)

- 得到cdsPosPDJoinIndexNonIndexDF数据集

- 然后join valuationWithoutAssest, 并drop掉重复的eodPositionId和AssetConstituentRecordID

  ```java
  cdsPosPDJoinIndexNonIndexDF.join(valuationWithoutAssest, 
                                   eodPositionId===eodPositionId,
                                  "left_outer")
  ```

- 得到最终posPDValJoinDFNonIndex

### (2) 针对CDS multiple underlying的情况



- 通过positionCDSDF.filter(POSITION_CONSTITUENT_FLAG_Y)过滤出对应的数据集posPDAssetJoinDFForIndex

- 先join AssetConstituent

  ```java
  posPDAssetJoinDFForIndex = posPDAssetJoinDFForIndex.join(assetConstituentDF,
                                                           underlyingassetId===constituentassetrootid,
                                                           "left_outer")
  ```

- 再join valuationWithAsset, 并drop掉重复列

  ```java
  posPDAssetValJoinDFForIndexDF = posPDAssetJoinDFForIndex
    .join(valuationWithAsset,
          eodpositionid===eodpositionid && AssetConstituentRecordId===AssetConstituentRecordid,
         "left_outer")
  ```

  

### (3) 合并两种情况

```java
posPDAssetValJoinDFForIndexDF.union(posPDValJoinDFNonIndex.selectExpr(posPDAssetValJoinDFForIndexDF.columns()))
```

得到posPDvalJoinDF

### (4) 对合并后的数据集执行cdsDataConsolidationAndNettingDerivation操作

"consolidation"（合并）和"netting derivation"（净额推导）是与合并和净额处理相关的概念。

- 对数据集执行consolidation和netting derivation, 转换为DrcStdConsolidationDataRow类型的JavaRDD

  ```java
  JavaRDD<DrcStdConsolidationDataRow> rowRDD = cdsDataConsolidationAndNettingProcessMain(posPDValJoinDF)
  ```

- 返回DrcStdConsolidationDataRow类型的DF

### (5) 得到结果drcNonSecCDSJoinDF

## 5. 处理加了PD数据的positionEQSwapDF

- 使用EQSwapConsolidationFunction处理生成DrcStdConsolidationDataRow类型的DF

- selectExpr(POS_PD_COLUMNS)
- 得到drcNonSecEqSwapJoinDF

## 6. 处理其他加了PD数据的otherPositions

- 添加一个新列"finaleodpositionid",列值为eodpositionid
- selectExpr(POS_PD_COLUMNS)
- 得到otherPositions

## 7. 合并drcNonSecEqSwapJoinDF和otherPositions

- 基于POS_PD_COLUMNS进行union
- 得到drcNonsecPosAndPDDF

## 8. 获得tradeMTM数据集

从valuationWithoutAsset中获取tradeMTM数据集，包含如下列：

***"EodPositionId",***

***"MarktoMarketMeasureLocalValue","MarktoMarketMeasureLocalCurrencyCode",***

***"MarktoMarketMeasureReportingValue","MarktoMarketMeasureReportingCurrencyCode"***

## 9. 处理CDS Option问题

### (1) 对于drcNonSecCDSJoinDF中isTradeValuationJoinNeeded==N的情况

需要将如下字段drop

“isTradeValuationJoinNeeded”

### (2) 对于drcNonSecCDSJoinDF中isTradeValuationJoinNeeded==Y的情况

需要将如下字段drop，再从tradeMTM中重新Join

“isTradeValuationJoinNeeded”

"MarktoMarketMeasureLocalValue","MarktoMarketMeasureLocalCurrencyCode",

"MarktoMarketMeasureReportingValue","MarktoMarketMeasureReportingCurrencyCode"

### (3) 合并上述两个结果集

得到drcNonSecCDSProcessedDF

## 10. 处理非CDS的drcNonsecPosAndPDDF

- 和tradeMTM进行Join
- 重命名相关列
- drop掉join的key

## 11. 将drcNonsecPosAndPDDF和assetConstituent进行join

- 通过PositionConstituentFlag进行判断是否进行join
- 对于标记为Y的通过drcNonsecPosAndPDDF.underlyingassetid==assetConstituentDF.constituentassetrootid进行join
- 对于标记为N的过滤出来不做处理
- 对上面两部分结果进行union,并将constituentweight类型转换为double
- 得到drcNonSecPosPDAssetDF

## 12. 在drcNonSecPosPDAssetDF加入valuationMeasurement

- 生成drcNonSecPosPDAssetValDF

## 13. 合并CDS和非CDS两个最终结果集

- union ( drcNonSecPosPDAssetValDF, drcNonSecCDSProcessedDF)

- 得到finalDF

# 二、生成INTERMIDATE_DRC_NONSEC数据集

通过在finalDF上执行map方法（DrcStdInterMediateRowMapFunction）

# 三、Join FCS_DATA

通过Join FCS_DATA 得到新的data

# 四、在前一步的结果集上执行extractDecorator

执行AggregationMeasureEnrichment的extract方法

主要是join数据集ELIGIBLE_LEGAL_ENTITY_DESCRIPTIONS

# 五、执行extractPriorSeniorityData

只是取出结果集WATERFALL_DATA以及WATERFALL_WORST_RATING_MAP加入到rdlSparkContext中备用

# 六、执行getConstituentEcptData

只是取出结果集ECPT_DATA_MAP加入到rdlSparkContext中备用

