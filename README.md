# Apache Spark Scala Tutorial For Korean
![](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

이 문서는 Spark 에서 Scala 언어를 사용하고자 하는 개발자를 위한 튜토리얼입니다.

이 문서를 읽기 위해서는 Java 또는 Python 에 대한 지식이 있어야 하고,  Java 또는 Python 을 이용해 Scala 를 이용한 경험이 있어야 합니다.

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

변수를 생성하는 키워드는 val 과 var 가 있습니다. val 로 생성한 변수는 값이 변경이 불가능한 변수가 됩니다. 반면에 var 로 생성한 변수는 값의 변경이 가능합니다. 하지만, Scala 에서는 var 를 **사용하지 않을 것**을 권장하고 있습니다.

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

### 함수 생성(declare function)

아래와 같이 함수를 생성할 수 있다.

```sh
scala> def addOne(m: Int): Int = m + 1
addOne: (m: Int)Int
scala> val three = addOne(2)
three: Int = 3
```

위에서 `=` 뒤 부분이 함수의 내용이지만 `return` 키워드가 없습니다. Scala 에서는 `return` 키워드의 생략을 권장합니다.

```sh
scala> def three() = 1 + 2
three: ()Int
scala> three()
res3: Int = 3
scala> three
res4: Int = 3
```

위에서 함수 정의에서도`1 + 2` 의 값이 정수이므로 `: Int` 가 생략되어도 정상 작동합니다.

파라미터가 없는 경우 `()` 를 생략할 수 있습니다.

### 클래스 생성(declare class)

아래 코드를 Scala 콘솔에 입력합니다.

```scala
class Calculator {
    val brand: String = "HP"
    def add(m: Int, n: Int): Int = m + n
}
```

```sh
scala> class Calculator {
     |   val brand: String = "HP"
     |   def add(m: Int, n: Int): Int = m + n
     | }
defined class Calculator

scala> val calc = new Calculator
calc: Calculator = Calculator@e75a11

scala> calc.add(1, 2)
res1: Int = 3

scala> calc.brand
res2: String = "HP"
```

필드(멤버 변수)는 val 로, 메소드(멤버 함수)는 def 로 정의합니다. var 로 필드를 생성할 수도 있지만 권장되지 않으므로 사용하지 않는 것이 좋습니다.

#### 클래스 생성자(class constructor)

Scala 에서 생성자는 괄호안 자체입니다.

```scala
class Calculator(brand: String) {
    println("start constructor")

    /**
    * 생성자
    */
    val color: String = if (brand == "TI") {
        "blue"
    } else if (brand == "HP") {
        "black"
    } else {
        "white"
    }

    // 인스턴스 메소드
    def add(m: Int, n: Int): Int = m + n

    println("end constructor")
}
```

위 코드를 Scala 콘솔에 입력하세요.

```sh
$ sbt console
scala> class Calculator(brand: String) {
     |     println("start constructor")
     |
     |     /**
     |     * 생성자
     |     */
     |     val color: String = if (brand == "TI") {
     |         "blue"
     |     } else if (brand == "HP") {
     |         "black"
     |     } else {
     |         "white"
     |     }
     |
     |     // 인스턴스 메소드
     |     def add(m: Int, n: Int): Int = m + n
     |
     |     println("end constructor")
     | }
defined class Calculator

scala> val calc = new Calculator("HP")
start constructor
end constructor
calc: Calculator = Calculator@524b86ce

scala> calc.color
res0: String = black
```

위 코드에서 `println` 이 두번 실행된 것을 볼 수 있습니다. 또한, `if` 문장이 리턴값을 반환해서 변수에 입력되고 있는 것을 볼 수 있습니다.
