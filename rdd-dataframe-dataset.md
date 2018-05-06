# RDD, DataFrame, DataSet 의 차이점

이 문서는 [A Tale of Three Apache Spark APIs: RDDs, DataFrames, and Datasets](https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html) 를 요약 정리합니다. RDD, DataFrame, Dataset 의 차이점 및 언제 어떤 포멧을 사용해야 하는지에 대해 설명합니다.

## RDD(Resilient Distributed Dataset)

`RDD` 는 Spark 의 핵심이 되는 데이타포멧으로, `DataFrame, Dataset` API 가 지원하지 않는 저레벨( low-level) 변환이 필요하거나, 데이타가 구조화할 수 없는 단순 텍스트같은 데이타거나, 구조화된 데이타의 이점과 최적화 및 퍼포먼스 향상에 관심이 없을 때 사용합니다.

> When to use RDDs?
> - you don’t care about imposing a schema, such as columnar format, while processing or accessing data attributes by name or column; and
> - you can forgo some optimization and performance benefits available with DataFrames and Datasets for structured and semi-structured data.

`RDD` 는 `Dataset` 대비 4배의 메모리를 차지하고, 속도 또한 느리기 때문에 가급적 사용하지 않을 것을 권장합니다.

## DataFrame, Dataset

`DataFrame` 은 데이타베이스의 테이블과 마찬가지로 컬럼기반의 데이타 처리가 가능합니다.

`Spark 2.0` 부터는 `DataFrame` 과 `Dataset` 은 단일화 됩니다. `Dataset` 의 하위에 `strongly-typed API` 와 `untyped API` 두가지 API 가 존재하게 되는데, 그 중 `DataFrame` 은 `untyped API` 로 작동합니다.

| Language | Main Abstraction                                |
|----------|-------------------------------------------------|
| Scala    | Dataset[T] & DataFrame (alias for Dataset[Row]) |
| Java     | Dataset[T]                                      |
| Python*  | DataFrame                                       |
| R*       | DataFrame                                       |

- Python, R 은 언어의 특성상 compile-time 타입체크를 지원하지 않아서 `untyped API` 만 존재합니다.

`DataFrame and Dataset API` 는 동일한 엔진 위에서 작동하므로, `R, Java, Scala, or Python` 에서 동일한 속도와 메모리 효율을 제공한다고 합니다.

> First, because DataFrame and Dataset APIs are built on top of the Spark SQL engine, it uses Catalyst to generate an optimized logical and physical query plan. Across R, Java, Scala, or Python DataFrame/Dataset APIs, all relation type queries undergo the same code optimizer, providing the space and speed efficiency.

`DataFrame, Dataset` 은 언제나 `RDD` 대비 속도향상과 메모리 효율을 가지므로, `DataFrame, Dataset` 을 사용할 것을 권장하며, compile-time 타입체크가 필요하면 `Dataset` 을 사용하라고 합니다.

> If you want higher degree of type-safety at compile time, want typed JVM objects, take advantage of Catalyst optimization, and benefit from Tungsten’s efficient code generation, use Dataset.