# Apache Spark Scala Tutorial For Korean
![](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

이 문서는 Spark 에서 Scala 언어를 사용하고자 하는 개발자를 위한 튜토리얼입니다.

이 문서를 읽기 위해서는 Java 에 대한 지식이 있어야 하고, Python 또는 Java 를 이용해 Scala 개발한 경험이 있어야 합니다.

문서의 양을 줄이기 위해 Spark 에 대한 설명은 생략되며, 또한 Scala 문법중 Spark 개발에 불필요하다고 판단되는 문법 또한 생략됩니다.

## 참고자료

아래 주소의 내용을 참고하였습니다.

* [HAMA 블로그](http://hamait.tistory.com/554)
* [스칼라 학교](https://twitter.github.io/scala_school/ko/index.html)
* [A free tutorial for Apache Spark.](https://github.com/deanwampler/spark-scala-tutorial)

## Scala 개발환경 구성하기

이 문서에서는 [SBT](http://www.scala-sbt.org/download.html) 를 이용해 샘플코드를 빌드하고 실행합니다. 툴의 다운로드 및 설치방법은 [여기](http://www.scala-sbt.org/download.html)를 참고하시기 바랍니다.

```sh
$ curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo
$ sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/
$ sudo yum install sbt
$ sbt
sbt:ec2-user> exit
$
```

## Hello, World! 출력하기

아래와 같은 방법으로 간단한 테스트 프로그램을 실행시킬 수 있습니다.

```sh
$ sbt
sbt:ec2-user> console
scala> println("Hello, World!")
Hello, World!
scala> :q
sbt:ec2-user> exit
$
```

## Scala 문법 설명하기

### 변수 생성(declare variable)

아래의 방법으로 변수를 생성할 수 있습니다.

```sh
$ sbt console
scala> val i: Int = 1
i: Int = 1
scala> i = 2                // error
scala> :q
$
```

변수를 생성하는 키워드는 val 과 var 가 있습니다. val 로 생성한 변수는 값이 변경이 불가능한 변수가 됩니다. 반면에 var 로 생성한 변수는 값의 변경이 가능합니다. 하지만, Scala 에서는 var 의 사용하지 않을 것을 권장하고 있습니다.

```sh
scala> val j = 2
j: Int = 2
```

Scala 에서는 변수값의 타입을 알 수 있는 경우(위에서 2 는 Int 이다.), 위와같이 변수타입을 생략할 수 있습니다.

```sh
scala> val k = 3
scala> println("class: " + k.getClass)
class: int
```

Scala 에서는 모든 변수는 객체입니다. 위에서 k 는 단순한 정수값이 아닌 정수형 객체가 됩니다.
