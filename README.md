# Apache Spark Scala Tutorial For Korean

![Spark](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

이 문서는 Spark 에서 Scala 언어를 사용하고자 하는 개발자를 위한 튜토리얼이다.

이 문서를 읽기 위해서는 Java 또는 Python 에 대한 지식이 있어야 하고,  Java 또는 Python 을 이용해 Spark 를 이용한 경험이 있어야 한다.

문서의 양을 줄이기 위해 Spark 에 대한 설명은 생략되며, 또한 Scala 문법중 Spark 개발에 불필요하다고 판단되는 문법 또한 생략한다.

## 참고자료

아래 주소의 내용을 참고한다.

* [HAMA 블로그](http://hamait.tistory.com/554)
* [스칼라 학교](https://twitter.github.io/scala_school/ko/index.html)
* [A free tutorial for Apache Spark.](https://github.com/deanwampler/spark-scala-tutorial)

## Scala 개발환경 구성하기

이 문서에서는 [SBT](http://www.scala-sbt.org/download.html) 를 이용해 샘플코드를 빌드하고 실행한다. 툴의 다운로드 및 설치방법은 [여기](http://www.scala-sbt.org/download.html)를 참고하기 바란다.

```sh
$ curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo
$ sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/
$ sudo yum install sbt
$ sbt
sbt:ec2-user> exit
$
```

## Hello, World! 출력하기

아래와 같은 방법으로 간단한 테스트 프로그램을 실행시킬 수 있다.

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

아래의 방법으로 변수를 생성할 수 있다.

```sh
$ sbt console
scala> val i: Int = 1
i: Int = 1
scala> i = 2                // error
scala> :q
$
```

변수를 생성하는 키워드는 val 과 var 가 있다. val 로 생성한 변수는 값의 변경이 불가능한 변수가 된다. 반면에 var 로 생성한 변수는 값의 변경이 가능하다. 하지만, Scala 에서는 var 를 **사용하지 않을 것**을 권장하고 있다.

```sh
scala> val j = 2
j: Int = 2
```

Scala 에서는 변수값의 타입을 알 수 있는 경우(위에서 2 는 Int 이다.), 위와같이 변수타입을 생략할 수 있다.

```sh
scala> val k = 3
scala> println("class: " + k.getClass)
class: int
```

Scala 에서는 모든 변수는 객체다. 위에서 k 는 단순한 정수값이 아닌 정수형 객체가 된다.

### 함수 생성(declare function)

아래와 같이 함수를 생성할 수 있다.

```sh
scala> def addOne(m: Int): Int = m + 1
addOne: (m: Int)Int
scala> val three = addOne(2)
three: Int = 3
```

위에서 `=` 뒤 부분이 함수의 내용이지만 `return` 키워드가 없다. Scala 에서는 `return` 키워드의 생략을 권장한다.

```sh
scala> def three() = 1 + 2
three: ()Int
scala> three()
res3: Int = 3
scala> three
res4: Int = 3
```

위에서 함수 정의에서도`1 + 2` 의 값이 정수이므로 `: Int` 가 생략되어도 정상 작동한다.

파라미터가 없는 경우 `()` 를 생략할 수 있다.

### 클래스 생성(declare class)

아래 코드를 Scala 콘솔에 입력해보자

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

필드(멤버 변수)는 val 로, 메소드(멤버 함수)는 def 로 정의한다. var 로 필드를 생성할 수도 있지만 권장되지 않으므로 사용하지 않는 것이 좋다.

#### 클래스 생성자(class constructor)

Scala 에서 생성자는 괄호안 자체이다.

```scala
class Calculator(brand: String) {
    /**
    * 생성자
    */
    println("start constructor")

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

위 코드를 Scala 콘솔에 입력해보자.

```sh
$ sbt console
scala> class Calculator(brand: String) {
     |     /**
     |     * 생성자
     |     */
     |     println("start constructor")
     |
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

위 코드에서 `println` 이 두번 실행된 것을 볼 수 있다.

또한, `if` 문장이 리턴값을 반환해서 변수에 입력되고 있는 것을 볼 수 있다.

#### 클래스 생성자의 파라미터를 맴버필드로 추가하기

생성자에 전달된 파라미터는 생성자가 실행된 후에는 사라진다.

```scala
class Person(name: String, age: Int)
val person = new Person("mong", 9)
println(person.age)                         // error
```

전달된 파라미터를 클래스의 맴버필드로 만들려면 아래와 같이 `val` 을 붙여주어야 한다.

```scala
class Person(val name: String, val age: Int)
val person = new Person("mong", 9)
println(person.age)                         // ok
```

get,set 메소드는 자동으로 추가되므로 별도로 작업할 필요가 없다.

### 패턴 매칭(switch case statment)

일반적인 switch case 문보다 더 많은 기능을 제공한다.

```scala
val times = 3

times match {
    case 1 => "one"
    case 2 => "two"
    case i if i == 3 => "three"
    case i if i == 4 => "four"
    case _ => "some other number"
}
```

단순히 정수매칭이나 문자열매칭 뿐만 아니라 조건문을 이용해 매칭할 수 있다.

마지막에 보이는 `_` 은 와일드카드로 사용된다. 여기서는 `case else` 로 사용되고 `import org.apache.spark.SparkContext._` 와 같은 경우에는 하위에 있는 모든 것을 임포트한다. 위에서 `case _` 가 없다면 매칭되는 값이 없을 때 에러가 발생한다.

#### 타입에 대한 패턴 매칭

값에 대한 매칭 뿐만 아니라 타입에 대해서도 패턴 매칭이 가능하다.

```scala
def bigger(o: Any): Any = {
    o match {
        case i: Int if i < 0 => i - 1
        case i: Int => i + 1
        case d: Double if d < 0.0 => d - 0.1
        case d: Double => d + 0.1
        case text: String => text + "s"
        case _ => "what is it?"
    }
}

bigger("cat")
```

위에서 정수 실수 뿐만 아니라 문자열과도 매칭함을 볼 수 있다.

#### 클래스에 대한 패턴 매칭

클래스에 대해서도 동일한 방식으로 패턴 매칭이 가능하다.

```scala
class Person(val name: String, val age: Int)

def isYoungPerson(person: Person) = person match {
    case p if p.age < 20 => "Yes"
    case _ => "No"
}
```

### 케이스 클래스(case class)

`case class` 를 이용하면 new 를 사용하지 않아도 클래스를 생성할 수 있다.

```scala
class Person(name: String, age: Int)
val a = Person("Lee", 21)               // error
val a = new Person("Lee", 21)           // ok
println(a.age)                          // error

case class Person(name: String, age: Int)
val a = Person("Lee", 21)               // ok
println(a.age)                          // ok
```

#### 케이스 클래스와 패턴 매칭

케이스 클래스는 패턴 매칭에 사용하기 위해 설계되었다.

```scala
case class Person(name: String, age: Int)

def isYoungPerson(person: Person) = person match {
    case Person("Lee", 12) => "Yes"
    case Person(_, 12) => "Yes"
    case _ => "No"
}

val p = Person("Lee", 12)
println(isYoungPerson(p))

val p2 = Person("Moon", 12)
println(isYoungPerson(p2))
```

위에서 `new` 키워드 없이 클래스가 생성됨을 볼 수 있다. 또한 와일드카드 문자인 `_` 가 클래스생성에도 사용되었음을 볼 수 있다.

### 기본 데이타셋(List, Set, Tuple)

List 에는 동일 타입의 데이타를 입력할 수 있고 중복된 데이타도 입력 가능하다. Set 에는 중복되는 데이타를 입력할 수 없다. Tuple 에는 서로 다른 타입의 데이타를 묶을 수 있다. Tuple 은 첫번째 데이타 호출에 `0` 이 아닌 `1` 을 사용하고 있다.

```scala
val numbers = List(1, 2, 3, 4)
println(numbers(2))

val animals = Set("Cat", "Dog", "Tiger")
println(animals("Cat"))

val hostPort = ("localhost", 80)
println(hostPort._1)

val a = 1 -> 2
println(a)
```

`->` 를 이용해 튜플을 생성할 수 있다.

### 기본 데이타셋(Map)

key-value 형태의 값의 묶음이 Map 이다.

```scala
val m = Map(1 -> "one", 2 -> "two")
println(m(2))
```

위에서 `->` 는 특별한 문법이 아니고 튜플의 생성에 불과하다. 위에서 생성된 맵은 실제로는 Map((1, "one"), (2, "two")) 의 형태가 되고, 맵에 들어있는 데이타는 첫번째 값이 key 가 되고, 두번째 값이 value 가 된다.

### 함수 조합(function combinator)

리스트를 전달받아 일정한 처리를 하고 처리된 값을 전달해주는 것을 합수조합이라고 한다.

#### map()

다른 언어에서는 `for (int i = 0; i < 10; i++) { ... }` 스타일로 코딩하는 경우가 많지만, Scala 에서는 변수 생성을 지양한다.

```scala
val numbers = List(1, 2, 3, 4)
println(numbers.map((i: Int) => i * 2))
println(numbers.map(i => i * i))
```

위와 같이 `for` 문 대신에 `map()` 을 이용해 입력된 데이타를 각 값들을 연산할 수 있다. 입력되는 데이타가 정수형이 확실하므로 `: Int` 는 생략할 수 있다. `map()` 과 별도의 함수를 조합할 수도 있다.

```scala
val numbers = List(1, 2, 3, 4)
def square(i: Int) = i * i
println(numbers.map(square _))
```

#### foreach()

`map()` 이 입력된 데이타를 그대로 두고 변형된 데이타를 반환하는것과 다르게, `foreach()` 는 입력된 값 자체를 변환하고 리턴값이 없다.

```scala
val numbers = List(1, 2, 3, 4)
println(numbers.foreach(i => i * i))
println(numbers)
```

`foreach()` 에게 리턴값을 요청하면 `Unit`(다른 언어에서는 `void`) 이 반환된다.

#### filter()

입력된 값을 필터링해서 값이 참인 것들로 이루어진 리스트를 반환한다.

```scala
val numbers = List(1, 2, 3, 4)
println(numbers.filter(i => i % 2 == 0))
```

#### zip()

두개의 리스트를 각각의 데이타를 묶어 튜플 리스트로 만든다.

```scala
val numbers = List(1, 2, 3, 4)
val animals = List("dog", "cat", "lion", "tiger")
println(numbers.zip(animals))
// List((1,dog), (2,cat), (3,lion), (4,tiger))

val numbers = List(1, 2, 3, 4)
val animals = List("dog", "cat", "lion")
println(numbers.zip(animals))
// List((1,dog), (2,cat), (3,lion))

val numbers = List(1, 2, 3)
val animals = List("dog", "cat", "lion", "tiger")
println(numbers.zip(animals))
// List((1,dog), (2,cat), (3,lion))
```

데이타의 갯수가 맞지 않으면 맞는 만큼만 묶어서 리스트를 반환한다.

#### partition()

`partition()` 는 입력된 리스트를 둘로 쪼개어 두개의 리스트를 반환한다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(numbers.partition(_ % 2 == 0))
val two = numbers.partition(_ % 2 == 0)
println(two._1)
```

반환되는 값은 튜플로 묶여 있다. 한개의 리스트를 이용하려면 튜플의 접근법과 동일하게 `._1` 또는 `._2` 를 이용하면 된다.

#### find()

`find()` 는 조건을 만족하는 첫번째 값을 반환한다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(numbers.find(i => i > 5))

val tup = List((1,"dog"), (2,"cat"), (3,"lion"))
println(tup.find(t => t._1 > 1 && t._2 == "lion"))
```

튜플을 입력값으로 받을 수 있다.

#### Option()

`Option()` 은 어떤 값이 있을 수도 있고 없을 수도 있을 때 사용된다. `find()` 에서 리턴되는 값이 `Option()` 이다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val res1 = numbers.find(i => i > 5)
val res2 = numbers.find(i => i > 10)

val result = if (res1.isDefined) { res1.get * 2 } else { 0 }
println(result)

val result = res1.getOrElse(0) * 2
println(result)
```

`find()` 는 리턴값이 없을 때 `None` 을 반환한다. 따라서 `res1.isDefined` 를 이용해 값이 있는지 체크하는 방법이 있다. 또는, `res1.getOrElse(0)` 를 이용해 디폴트값을 지정해 줄 수도 있다.

#### drop(), dropWhile()

`drop()` 은 입력되는 리스트에서 앞에서 `n` 개의 값을 없앤 나머지 리스트를 반환한다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(numbers.drop(5))
println(numbers.drop(20))

println(numbers.dropWhile(_ % 2 != 0))
```

`dropWhile()` 은 조건을 만족하지 않는 값이 있을 때까지의 값을 없앤 나머지 리스트를 반환한다. 위에서 2 에서 조건을 만족하지 않아 drop 을 중단하고 나머지 리스트를 반환하게 된다.

#### foldLeft()

`foldLeft()` 는 입력되는 리스트의 각 값들을 연산한 값의 누적값을 반환한다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(numbers.foldLeft(0) { (acc, i) => println("acc: " + acc + " i: " + i); acc + i })
println(numbers.foldLeft(1000) { (acc, i) => println("acc: " + acc + " i: " + i); acc + i })
// acc: 1000 i: 1
// acc: 1001 i: 2
// acc: 1003 i: 3
// acc: 1006 i: 4
// acc: 1010 i: 5
// acc: 1015 i: 6
// acc: 1021 i: 7
// acc: 1028 i: 8
// acc: 1036 i: 9
// acc: 1045 i: 10
// 1055
```

위에서 `0, 1000` 은 시작값이 되고, acc 에 누적값이 저장되며, i 가 입력된 리스트의 데이타이다.

#### foldRight()

`foldRight()` 는 `foldLeft()` 와 동일한 기능을 하는데 방향만 거꾸로이다.

```scala
val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
println(numbers.foldRight(1000) { (acc, i) => println("acc: " + acc + " i: " + i); acc + i })
```

#### flatten()

`flatten()` 은 입력된 데이타의 중첩(nested)단계를 한단계 풀어준다.

```scala
val nestedNumbers = List(List(1, 2), List(3, 4), List(5, 6))
println(nestedNumbers.flatten)
// List(1, 2, 3, 4, 5, 6)
```

파라미터가 없는 함수의 경우 `()` 를 생략할 수 있다.

#### flatMap()

`flatMap()` 은 `flatten()` 과 `map()` 을 합친것이다.

```scala
val nestedNumbers = List(List(1, 2), List(3, 4), List(5, 6))
println(nestedNumbers.flatMap(x => x.map(_ * 2)))
// List(2, 4, 6, 8, 10, 12)
```

리스트의 각 데이타에 대해 `map()` 을 적용하고 리턴된 값들을 `flatten()` 한다.

## Scala Spark Example With Web Log

Spark Shell 을 구동한다.

```sh
$ spark-shell
Spark context Web UI available at http://ip-172-31-16-104.ap-northeast-2.compute.internal:4040
Spark context available as 'sc' (master = local[*], app id = local-1524908007684).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.3.0
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_161)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

## 웹로그 살펴보기

웹로그는 공개할 수가 없어 개인적으로 구하기 바랍니다.

```scala
scala> val log_RDD = sc.textFile("/home/ec2-user/dev/www2-www-18041917.gz")
```

로그파일은 로그의 첫부분에 로그파일의 포멧정보가 있다.

```scala
scala> log_RDD.take(5).map(line => println(line))
#Software: Microsoft Internet Information Services 7.5
#Version: 1.0
#Date: 2018-04-19 08:00:00
#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) cs(Referer) sc-status sc-substatus sc-win32-status time-taken
2018-04-19 08:00:00 110.93.XXX.83 GET /login/loginpage.asp vType=G 80 - 106.XXX.166.106 Mozilla/5.0+(Windows+NT+6.1;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko http://www.test.co.kr/ 302 0 0 0
```

공백문자로 쪼갤 수 있게 되어 있다. 로그포멧은 서버설정에 의해 결정되는데 위의 경우 총 15개의 필드가 있고 9번째에 클라이언트 아이피가 있다. 이런 정보를 바탕으로 클라이언트 아이피별 조회건수를 구해보자.

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)
val filtered_log_array = log_RDD.take(50)
       .map(line => line.split(" "))
       .filter(line => line.size == 15)
       .map(arr => (arr(0), arr(1), arr(8)))
ctrl-D

scala> val df = sc.parallelize(filtered_log_array).toDF("date", "time", "clientip")

scala> df.show()
```
