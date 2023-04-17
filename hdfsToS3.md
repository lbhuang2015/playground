# 从HDFS拷贝文件到S3存储

## 1. 如何从on-premise的hdfs集群拷贝文件到s3存储

要将数据从On-premise的HDFS集群复制到Amazon S3，可以使用以下步骤：

1. 在On-premise的HDFS集群上安装AWS CLI并配置。AWS CLI是Amazon Web Services（AWS）的命令行接口，它允许您通过命令行与S3存储进行交互。
2. 使用Hadoop DistCp命令将数据从HDFS复制到S3存储。 DistCp是一个用于在Hadoop集群之间复制数据的工具。您可以使用它将数据从On-premise的HDFS集群复制到S3存储。
3. 在使用DistCp之前，需要创建一个IAM角色，并在该角色中设置S3访问权限。可以使用AWS IAM控制台创建此角色。

以下是基本的DistCp命令格式：

```shell
hadoop distcp hdfs://source/path s3://destination/path
```

其中，source是要复制的HDFS路径，destination是S3存储桶和目标路径。您可以在此命令中指定其他选项，例如并行度和重试次数。

1. 运行命令后，DistCp将开始将数据从HDFS集群复制到S3存储。这可能需要一些时间，具体取决于数据量和网络速度。
2. 复制完成后，可以在S3控制台中验证已复制的数据。

## 2. 请用scala编写一个样例程序

从指定hdfs位置拷贝一个数据目录及其下子目录和所包含的parquet文件，将其拷贝到s3存储，拷贝成功后删除hdfs上相应的源数据目录文件。该程序需要支持每天定时启动并执行.

### (1) 方式1

```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.s3._
import akka.stream.{ActorMaterializer, Materializer}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

object HdfsToS3 {

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("hdfs-to-s3-system")
    implicit val mat: Materializer = ActorMaterializer()
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val hadoopConfig = new Configuration()
    val hdfsPath = new Path("/path/to/source/directory") // 指定HDFS源路径
    val s3BucketName = "my-s3-bucket" // 指定S3存储桶名称
    val s3DestinationPath = "path/to/s3/destination" // 指定S3目标路径

    val files = getFiles(hadoopConfig, hdfsPath) // 获取HDFS源路径下的所有Parquet文件
    val s3Sink = S3.multipartUpload(s3BucketName, s3DestinationPath) // 创建S3上传sink

    Source(files) // 创建数据源
      .mapAsync(1) { file =>
        val s3Key = file.getName // 使用文件名作为S3的key
        val s3PutRequest = S3Request(PutObjectRequest(s3BucketName, s3Key, file)) // 创建S3请求
        S3.upload(s3PutRequest, s3Sink) // 将文件上传到S3
      }
      .runWith(Sink.ignore) // 执行上传

    val fs = FileSystem.get(hadoopConfig)
    fs.delete(hdfsPath, true) // 删除HDFS源路径下的所有文件

    system.scheduler.schedule(
      initialDelay = 1.day, // 延迟1天
      interval = 1.day // 每天执行一次
    ) {
      val files = getFiles(hadoopConfig, hdfsPath)
      if (files.nonEmpty) {
        // 与上传S3和删除HDFS源路径的代码相同
      }
    }

    Await.result(system.whenTerminated, Duration.Inf) // 等待系统终止
  }

  def getFiles(hadoopConfig: Configuration, path: Path): Seq[Path] = {
    val fs = path.getFileSystem(hadoopConfig)
    val statuses = fs.listStatus(path)
    val files = statuses.filter(_.isFile).map(_.getPath)
    val directories = statuses.filter(_.isDirectory).map(_.getPath)
    files ++ directories.flatMap(getFiles(hadoopConfig, _))
  }
}

```



### (2) 方式2

以下是一个使用Scala编写的样例程序，用于从指定的HDFS位置复制数据目录及其下子目录和所包含的Parquet文件，将其复制到S3存储并删除HDFS上相应的源数据目录文件。该程序还使用了Cron表达式进行调度，以在每天指定的时间启动并执行。

```scala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{DeleteObjectRequest, PutObjectRequest}
import com.cronutils.model.Cron
import com.cronutils.parser.CronParser

object HdfsToS3CopyApp {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val hadoopConf = fs.getConf
    val spark = SparkSession.builder().config(hadoopConf).getOrCreate()
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build()

    val sourceHdfsPath = new Path(args(0))
    val s3BucketName = args(1)
    val s3DestinationKey = args(2)

    // Delete s3 destination folder before copy
    s3Client.listObjects(s3BucketName, s3DestinationKey).getObjectSummaries.forEach(
      objectSummary => {
        s3Client.deleteObject(new DeleteObjectRequest(s3BucketName, objectSummary.getKey))
      }
    )

    FileUtil.copy(fs, sourceHdfsPath, fs, new Path(s"file:///tmp/${sourceHdfsPath.getName}"), false, conf)

    // Copy files to S3
    val s3DestinationPath = s"$s3DestinationKey/${sourceHdfsPath.getName}"
    val localSourcePath = s"file:///tmp/${sourceHdfsPath.getName}"
    val filesToCopy = fs.listStatus(new Path(localSourcePath)).filter(_.getPath.toString.endsWith(".parquet"))

    for (file <- filesToCopy) {
      s3Client.putObject(new PutObjectRequest(s3BucketName, s"$s3DestinationPath/${file.getPath.getName}", file.getPath.toFile))
    }

    // Delete source directory
    fs.delete(sourceHdfsPath, true)

    spark.stop()
  }

  def runJob(cronExpression: String, args: Array[String]): Unit = {
    val parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX))
    val cron = parser.parse(cronExpression)
    val job = new Runnable() {
      def run() {
        main(args)
      }
    }
    val nextRun = cron.nextExecutionTime()
    val delay = nextRun.getTime - System.currentTimeMillis()
    new java.util.Timer().schedule(
      new java.util.TimerTask() {
        def run() {
          job.run()
        }
      },
      delay
    )
  }

  def main(args: Array[String]): Unit = {
    runJob("0 0 12 * * ?", args) // Run job at 12:00 PM every day
  }
}

```

请注意，上述代码使用了AWS SDK for Java和CronUtils库。在运行程序之前，需要将这些依赖项添加到项目中。此外，您需要将AWS凭据配置为环境变量或在代码中

上述样例代码不支持多线程并发，因为它使用的是Java的Timer对象来执行定时任务，它只能支持单线程的执行方式。如果要实现多线程并发，需要使用一些支持并发的定时任务库，例如Quartz、Akka Scheduler或者Cron4s等。同时，在拷贝文件到S3存储的过程中，您可以使用多线程的方式并发地上传多个文件，以加快上传速度。在Scala中，您可以使用Scala Futures、Akka Actors或者Java的Executor框架来实现并发上传。

###  (3) 使用Scala futures实现多线程模式

以下是使用Scala Futures实现的上述样例代码的多线程版本，它支持并发上传多个文件，以加快上传速度

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{DeleteObjectRequest, PutObjectRequest}
import com.cronutils.model.Cron
import com.cronutils.parser.CronParser

object HdfsToS3CopyApp {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val hadoopConf = fs.getConf
    val spark = SparkSession.builder().config(hadoopConf).getOrCreate()
    val s3Client: AmazonS3 = AmazonS3ClientBuilder.standard().withCredentials(new DefaultAWSCredentialsProviderChain()).build()

    val sourceHdfsPath = new Path(args(0))
    val s3BucketName = args(1)
    val s3DestinationKey = args(2)

    // Delete s3 destination folder before copy
    s3Client.listObjects(s3BucketName, s3DestinationKey).getObjectSummaries.forEach(
      objectSummary => {
        s3Client.deleteObject(new DeleteObjectRequest(s3BucketName, objectSummary.getKey))
      }
    )

    FileUtil.copy(fs, sourceHdfsPath, fs, new Path(s"file:///tmp/${sourceHdfsPath.getName}"), false, conf)

    // Copy files to S3 using futures
    val s3DestinationPath = s"$s3DestinationKey/${sourceHdfsPath.getName}"
    val localSourcePath = s"file:///tmp/${sourceHdfsPath.getName}"
    val filesToCopy = fs.listStatus(new Path(localSourcePath)).filter(_.getPath.toString.endsWith(".parquet"))

    val futures = for (file <- filesToCopy) yield {
      Future {
        val key = s"$s3DestinationPath/${file.getPath.getName}"
        val request = new PutObjectRequest(s3BucketName, key, file.getPath.toFile)
        s3Client.putObject(request)
        key
      }
    }

    val futureResult = Future.sequence(futures)

    futureResult onComplete {
      case Success(results) =>
        println(s"Successfully uploaded files: ${results.mkString(", ")}")
        // Delete source directory
        fs.delete(sourceHdfsPath, true)
      case Failure(ex) =>
        println(s"Failed to upload files to S3: ${ex.getMessage}")
    }

    spark.stop()
  }

  def runJob(cronExpression: String, args: Array[String]): Unit = {
    val parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX))
    val cron = parser.parse(cronExpression)
    val job = new Runnable() {
      def run() {
        main(args)
      }
    }
    val nextRun = cron.nextExecutionTime()
    val delay = nextRun.getTime - System.currentTimeMillis()
    new java.util.Timer().schedule(
      new java.util.TimerTask() {
        def run() {
          job.run()
        }
      },
      delay
    )
  }

  def main(args: Array[String]): Unit = {
    runJob("0 0 12 * * ?", args) // Run job at 12:00 PM every day
  }
}
```

在上述代码中,我们将文件拷贝到本地临时目录后，使用`fs.listStatus`方法列出所有需要拷贝到S3的文件。然后，我们使用`for`循环遍历所有需要拷贝的文件，并将每个文件都放入一个Scala Future对象中。接着，我们将所有Future对象传递给`Future.sequence`方法，以便在它们全部完成时获取结果。`Future.sequence`方法将返回一个包含所有Future对象结果的序列，其中的每个元素都对应于一个Future对象。

在将所有文件上传到S3后，我们使用`Future`的`onComplete`方法注册了一个回调函数，以在上传完成或上传失败时执行相应的操作。如果所有文件都成功上传到S3，我们将打印一条成功上传的消息，并删除HDFS上的源目录。如果上传过程中发生任何错误，我们将打印一条错误消息。

最后，我们在`runJob`方法中使用Java的Timer对象执行定时任务。在本例中，我们使用Cron表达式`"0 0 12 * * ?"`指定每天中午12点启动程序。

### (4) 使用akka实现

请给出一个使用akka s3组件实现上述要求的Scala样例代码

下面是使用Akka S3组件实现上述要求的Scala样例代码。在本例中，我们将使用Akka S3组件中提供的`S3`客户端对象，它提供了一种简单的方式来上传和下载文件，以及管理S3桶中的对象。

```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.{ListBucketVersion2, S3Headers}
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.amazonaws.services.s3.model.{Bucket, S3ObjectSummary}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object CopyHdfsToS3WithAkkaS3 {

  val hadoopConf: Configuration = new Configuration()
  val fs: FileSystem = FileSystem.get(hadoopConf)

  def copyDirectoryToS3(dir: Path, s3Bucket: String, s3Prefix: String)(implicit system: ActorSystem, mat: ActorMaterializer): Future[Unit] = {
    val s3Client = S3.factory()

    // list all files under the directory
    val fileStatus = fs.listStatus(dir)
    val filePaths = fileStatus.map(_.getPath)

    // filter out the directories and only select the parquet files
    val parquetPaths = filePaths.filter(path => fs.isFile(path) && path.toString.endsWith(".parquet"))

    // create a source with all the parquet file paths
    val source = Source(parquetPaths.toSeq)

    // create a S3 sink that uploads each file
    val s3Sink = S3.multipartUpload(s3Bucket, s3Prefix)

    // use the source and sink to upload all the files concurrently
    source.map { path =>
      val s3ObjectKey = s"$s3Prefix/${path.getName}"
      val s3Headers = S3Headers(ServerSideEncryption.AES256)
      S3ArchiveMetadata(s3ObjectKey, s3Headers) -> fs.open(path)
    }
      .runWith(s3Sink)

      // if all files are uploaded successfully, delete the source directory
      .flatMap { _ =>
        fs.delete(dir, true)
        println(s"All files from ${dir.toString} have been uploaded to S3")
        Future.successful(())
      }
      // if there is an error, print an error message and return a failed future
      .recoverWith { case ex: Throwable =>
        println(s"Failed to upload files from ${dir.toString} to S3: ${ex.getMessage}")
        Future.failed(ex)
      }
  }

  def runJob(s3Bucket: String, s3Prefix: String)(implicit system: ActorSystem, mat: ActorMaterializer): Unit = {
    // set up the schedule to run at noon every day
    val schedule = "0 0 12 * * ?"
    val timer = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run(): Unit = {
        println(s"Starting job at ${java.time.Instant.now()}")

        // copy the directory to S3
        val dir = new Path("/path/to/hdfs/directory")
        copyDirectoryToS3(dir, s3Bucket, s3Prefix)
      }
    }
    timer.scheduleAtFixedRate(task, 0, org.quartz.CronExpression.parse(schedule).getNextValidTimeAfter(new java.util.Date()).getTime - System.currentTimeMillis())
  }

    def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val mat: ActorMaterializer = ActorMaterializer()

    val s3Bucket = "my-s3-bucket"
    val s3Prefix = "data"
    
    // start the job
    runJob(s3Bucket, s3Prefix)

    // keep the application running
    while (true) {
      Thread.sleep(1000)
    }
  }

```

这个 `main` 方法启动了一个调度任务来定期运行 `copyDirectoryToS3` 函数，并且通过一个无限循环来保持应用程序运行。请注意，这个调度程序会在每天中午 12 点启动一次。如果需要更改调度程序的时间，请修改 `schedule` 变量的值。

上述使用Akka从HDFS上传文件到S3的样例程序，需要在sbt项目文件中包含相关依赖，请给出相关依赖

是的，这个样例程序需要以下依赖项：

- `com.typesafe.akka" %% "akka-actor" % "2.6.16"`
- `com.typesafe.akka" %% "akka-stream" % "2.6.16"`
- `software.amazon.awssdk" % "s3" % "2.17.4"`
- `com.adobe.hadoop" % "parquet-hadoop-bundle" % "1.11.1"`

你可以在 `build.sbt` 文件中添加以下行来将这些依赖项添加到项目中：

```scala
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.6.16",
  "com.typesafe.akka" %% "akka-stream" % "2.6.16",
  "software.amazon.awssdk" % "s3" % "2.17.4",
  "com.adobe.hadoop" % "parquet-hadoop-bundle" % "1.11.1"
)
```

需要注意的是，如果你在本地环境中运行此程序，还需要在 `build.sbt` 文件中添加以下依赖项：

- `org.apache.hadoop" % "hadoop-client" % "3.3.1"`
- `org.apache.hadoop" % "hadoop-common" % "3.3.1"`

因为 HDFS 客户端和相关依赖项不是默认包含在 Akka 中的。你需要手动添加这些依赖项才能在本地运行程序。

请在使用scala和akka构建的从hdfs向aws s3拷贝文件的案例中使用异常处理机制捕获并处理可能的异常

### (5) 异常捕获

在使用 Scala 和 Akka 构建的从 HDFS 向 AWS S3 拷贝文件的案例中，我们可以使用异常处理机制来捕获可能发生的异常，例如连接错误、文件读写错误等等。

以下是使用异常处理机制修改后的样例代码：

```scala
import akka.actor.ActorSystem
import akka.stream.alpakka.s3.S3Attributes
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{FileIO, Keep, Source}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.IOException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object HdfsToS3Transfer {
  def main(args: Array[String]): Unit = {
    val hdfsPath = args(0)
    val s3Path = args(1)
    val accessKey = args(2)
    val secretKey = args(3)

    val hadoopConfig = new Configuration()
    val hdfsFileSystem = FileSystem.get(hadoopConfig)
    val s3Config = ConfigFactory.parseString(s"""
        |aws {
        |  credentials {
        |    access-key-id = "$accessKey"
        |    secret-access-key = "$secretKey"
        |  }
        |  s3 {
        |    region {
        |      provider = "static"
        |      default-region = "us-west-2"
        |    }
        |  }
        |}
        |""".stripMargin)
    implicit val system: ActorSystem = ActorSystem("S3ActorSystem", s3Config)
    val s3Sink = S3.multipartUpload(s3Path)
      .withAttributes(S3Attributes.settings(s3Config))

    val hdfsPathObj = new Path(hdfsPath)
    val fileList = listFiles(hdfsFileSystem, hdfsPathObj)

    val futures = fileList.map { filePath =>
      Future {
        try {
          val input = hdfsFileSystem.open(filePath)
          val source: Source[ByteString, Any] = Source.fromIterator(() => input)
            .map(ByteString(_))
          val (uploadResult, _) = source
            .toMat(s3Sink)(Keep.both)
            .run()
          println(s"Transferred file $filePath to S3 successfully. ETag: ${uploadResult.eTag}")
          hdfsFileSystem.delete(filePath, false)
        } catch {
          case ex: IOException => println(s"Error transferring file $filePath: ${ex.getMessage}")
        }
      }
    }
    Future.sequence(futures).onComplete(_ => {
      hdfsFileSystem.close()
      system.terminate()
      println("All files transferred to S3 successfully.")
    })
  }

  def listFiles(fileSystem: FileSystem, path: Path): List[Path] = {
    val status = fileSystem.listStatus(path)
    val files = status.filter(_.isFile).map(_.getPath).toList
    val dirs = status.filter(_.isDirectory).map(_.getPath)
    files ++ dirs.flatMap(listFiles(fileSystem, _))
  }
}
```

在上面的代码中，我们使用 try-catch 语句块来捕获可能发生的 `IOException` 异常。如果发生了异常，我们将输出异常信息。如果没有发生异常，我们将输出文件传输成功的信息，包括文件的 ETag。同时，我们还会在完成文件传输后关闭 HDFS 和 Akka 的 ActorSystem。

### (6) Scala中可以使用Spring注解

Scala可以@ConfigurationProperties注解来读取配置文件

在Scala中，可以使用`@ConfigurationProperties`注解来读取配置文件。这个注解是Spring框架提供的，用于将配置文件中的属性值映射到一个Java或Scala对象的属性中。

以下是一个使用`@ConfigurationProperties`注解读取配置文件的示例：

首先，在`application.yml`或`application.properties`文件中定义属性：

```yaml
aws:
  s3:
    access-key: my-access-key
    secret-key: my-secret-key
    bucket-name: my-bucket-name
```

然后，在Scala类中定义一个对象，用`@ConfigurationProperties`注解标注这个对象，并指定`prefix`属性为配置文件中的前缀，以便将配置文件中的属性映射到这个对象的属性中：

```scala
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "aws.s3")
class S3Config {
  var accessKey: String = _
  var secretKey: String = _
  var bucketName: String = _
}
```

最后，在需要使用这些配置属性的地方，通过注入这个对象的方式获取配置属性的值：

```scala
import org.springframework.beans.factory.annotation.Autowired

class S3Service @Autowired()(s3Config: S3Config) {
  def uploadFileToS3(file: File): Unit = {
    val s3Client = AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(s3Config.accessKey, s3Config.secretKey)))
      .withRegion(Regions.US_EAST_1)
      .build()

    val objectKey = file.getName
    s3Client.putObject(s3Config.bucketName, objectKey, file)
  }
}
```

这样，就可以使用`@ConfigurationProperties`注解来读取配置文件中的属性值，使代码更加简洁和易于维护。

## 3. distcp

可以使用Hadoop的distcp工具，它支持在Hadoop集群之间复制大量数据，而无需将数据复制到客户端。在这种情况下，数据在集群中流动，而不是通过客户端。可以使用类似以下命令将数据从HDFS复制到S3：

```
phpCopy code
hadoop distcp hdfs://<hdfs-hostname>:<port>/<hdfs-path> s3a://<s3-bucket>/<s3-path>
```

需要将 `<hdfs-hostname>`，`<port>`，`<hdfs-path>`，`<s3-bucket>` 和 `<s3-path>` 替换为相应的值。注意，需要在Hadoop集群中安装S3A文件系统以便使用S3作为目标。

若HDFS已经安装了S3相关支持，可以使用Hadoop S3A Connector来实现从HDFS直接拷贝文件到S3系统，无需经过程序所在服务器内存。

以下是一个基于Scala和Hadoop S3A Connector的示例代码：

```scala
scalaCopy code
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AFileSystem

import java.security.MessageDigest
import java.util.Base64
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object HdfsToS3DirectCopy {
  val hdfsUri = "hdfs://your-hdfs-uri"
  val s3Endpoint = "your-s3-endpoint"
  val s3AccessKey = "your-s3-access-key"
  val s3SecretKey = "your-s3-secret-key"
  val s3Bucket = "your-s3-bucket"

  def main(args: Array[String]): Unit = {
    val hdfsPath = new Path("/path/to/hdfs/file")
    val s3Path = new Path(s"s3a://$s3Bucket/path/to/s3/file")

    val fs = FileSystem.get(new java.net.URI(hdfsUri), new org.apache.hadoop.conf.Configuration())

    val hdfsInputStream = fs.open(hdfsPath)
    val s3FileSystem = new S3AFileSystem()
    s3FileSystem.initialize(new java.net.URI(s3Endpoint), fs.getConf)

    val s3OutputStream = s3FileSystem.create(s3Path)

    // 将hdfs文件流写入s3文件流
    hdfsInputStream.transferTo(s3OutputStream)

    // 关闭文件流
    hdfsInputStream.close()
    s3OutputStream.close()

    // 获取文件MD5值并比较
    val hdfsMd5Future = Future { getFileMd5(hdfsPath, fs) }
    val s3Md5Future = Future { getFileMd5(s3Path, s3FileSystem) }

    val compareResultFuture = for {
      hdfsMd5 <- hdfsMd5Future
      s3Md5 <- s3Md5Future
    } yield compareMd5s(hdfsMd5, s3Md5)

    compareResultFuture.onComplete {
      case Success(compareResult) =>
        if (compareResult) {
          println("File transfer and MD5 check succeed!")
        } else {
          println("MD5 check failed!")
        }
      case Failure(exception) => println(s"Error occurred: ${exception.getMessage}")
    }

    // 关闭文件系统连接
    fs.close()
    s3FileSystem.close()
  }

  def getFileMd5(path: Path, fs: FileSystem): String = {
    val inputStream = fs.open(path)
    val md5Digest = MessageDigest.getInstance("MD5")
    val buffer = new Array[Byte](1024 * 1024)

    Stream
      .continually(inputStream.read(buffer))
      .takeWhile(_ != -1)
      .foreach(md5Digest.update(buffer, 0, _))

    inputStream.close()

    Base64.getEncoder.encodeToString(md5Digest.digest())
  }

  def compareMd5s(md5A: String, md5B: String): Boolean = {
    md5A.equals(md5B)
  }
}
```

其中，使用了`org.apache.hadoop.fs.s3a.S3AFileSystem`来实现S3文件系统的连接和文件操作，并且利用`Future`实现异步操作。

以下代码：

```scala
import java.io.File
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.async.Async.{async, await}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger

def uploadFilesToS3(fileStatusList: List[FileStatus], s3Bucket: String, s3Prefix: String): List[(String, Boolean)] = {
  // 创建 Hadoop 配置对象
  val hadoopConf = new org.apache.hadoop.conf.Configuration()

  // 创建 Hadoop 文件系统对象
  val hdfs = FileSystem.get(hadoopConf)

  // 创建 Amazon S3 客户端对象
  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
    .build()

  // 创建线程池和执行上下文对象
  val threadPool = Executors.newFixedThreadPool(Runtime.getRuntime.availableProcessors())
  implicit val ec = ExecutionContext.fromExecutor(threadPool)

  // 使用 Future 和 async/await 机制上传文件
  val futureList = fileStatusList.map { fileStatus =>
    async {
      val hdfsPath = fileStatus.getPath.toString
      val s3Key = s3Prefix + fileStatus.getPath.getName
      try {
        s3Client.putObject(s3Bucket, s3Key, new File(hdfsPath))
        Logger.getLogger(getClass).info(s"Copied file $hdfsPath to S3 bucket $s3Bucket with key $s3Key")
        (fileStatus.getPath.getName, true)
      } catch {
        case ex: Exception =>
          Logger.getLogger(getClass).error(s"Failed to copy file $hdfsPath to S3 bucket $s3Bucket with key $s3Key: ${ex.getMessage}")
          (fileStatus.getPath.getName, false)
      }
    }
  }

  // 等待所有 Future 完成，并返回结果列表
  val resultList = await(Future.sequence(futureList))

  // 关闭线程池
  threadPool.shutdown()

  resultList
}
```
以下是一个使用org.apache.hadoop.tools.DistCpOptions上传文件从HDFS到S3兼容系统的Java示例代码：

java
Copy code
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

public class HdfsToS3DistCp {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path srcPath = new Path("hdfs://your-hdfs-path");
        Path destPath = new Path("s3a://your-s3-compatible-path");
        
        DistCpOptions options = new DistCpOptions.Builder()
            .withSyncFolder(true)
            .withOverwrite(true)
            .withDeleteMissing(true)
            .withBlocking(true)
            .withNumListstatusThreads(20)
            .withMaxMaps(50)
            .withMapBandwidth(100)
            .build();
        
        DistCp distCp = new DistCp(conf, options);
        distCp.execute(new Path[]{srcPath, destPath});
    }
}
在这个示例代码中，我们使用了org.apache.hadoop.tools.DistCp类来执行文件拷贝操作，并使用org.apache.hadoop.tools.DistCpOptions类来设置拷贝选项。在选项中，我们开启了同步文件夹、覆盖目标文件、删除目标中不存在的文件等选项，以保证文件拷贝的准确性和完整性。

要注意的是，S3是一个分布式存储系统，与HDFS有所不同，需要使用特定的S3文件系统实现。在示例代码中，我们使用了S3A文件系统实现，可以通过hadoop-aws依赖包引入。在使用S3A文件系统时，需要在core-site.xml中配置S3的访问密钥和密钥ID，具体可以参考Apache Hadoop官方文档。

此外，在执行上述代码时，需要保证当前节点有访问HDFS和S3兼容系统的权限。



Eric H
上述操作是否使用了distcp的分布式多任务特性

是的，上述示例代码使用了DistCp的分布式多任务特性，可以并行执行多个Mapper任务。这些任务可以在集群中不同的节点上运行，从而充分利用集群的计算和存储资源，提高数据拷贝的效率和速度。

在示例代码中，我们使用了DistCpOptions中的withMaxMaps()方法设置了最大Mapper任务数，同时也使用了withMapBandwidth()方法设置了每个Mapper的带宽，以控制并发的任务数量和数据拷贝的速率。这些参数可以根据实际情况进行调整，以达到最佳的数据拷贝效果。

另外需要注意的是，DistCp的多任务特性默认是开启的，因此只需要通过withMaxMaps()方法设置最大Mapper任务数即可。在集群中，可以通过调整mapreduce.job.maps参数来控制总的Mapper任务数，以更好地利用集群资源。



Eric H
请使用scala实现上述功能并加入content md5检验功能

以下是一个使用Scala编写的示例代码，实现了从HDFS上传文件到S3兼容系统，并加入了Content-MD5检验的功能：

scala
Copy code
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.apache.hadoop.io.{BytesWritable, MD5Hash}

import java.io.BufferedInputStream
import java.nio.ByteBuffer
import java.security.MessageDigest
import scala.collection.JavaConverters._

object HdfsToS3DistCp {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val srcPath = new Path("hdfs://your-hdfs-path")
    val destPath = new Path("s3a://your-s3-compatible-path")

    val options = new DistCpOptions.Builder()
      .withSyncFolder(true)
      .withOverwrite(true)
      .withDeleteMissing(true)
      .withBlocking(true)
      .withNumListstatusThreads(20)
      .withMaxMaps(50)
      .withMapBandwidth(100)
      .build()

    val distCp = new DistCp(conf, options)

    // 添加Content-MD5检验
    val md5s = fs.listFiles(srcPath, true)
      .asScala
      .filter(_.isFile)
      .map(fileStatus => {
        val path = fileStatus.getPath
        val inputStream = fs.open(path)
        val md5 = MessageDigest.getInstance("MD5")
        val buf = new Array[Byte](1024 * 1024)
        var read = inputStream.read(buf)
        while (read != -1) {
          md5.update(buf, 0, read)
          read = inputStream.read(buf)
        }
        inputStream.close()
        val md5Hash = new MD5Hash(md5.digest())
        (path, md5Hash)
      }).toMap

    val results = distCp.execute(Array(srcPath, destPath))

    // 验证上传的文件的Content-MD5
    results.asScala.foreach(result => {
      val srcPath = result.getSourcePath
      val destPath = result.getTargetPath
      val contentMD5 = result.getContentMD5
      val expectedMD5 = md5s.getOrElse(srcPath, null)
      if (expectedMD5 == null) {
        println(s"WARNING: Cannot find MD5 for source file $srcPath.")
      } else if (!expectedMD5.equals(new MD5Hash(ByteBuffer.wrap(contentMD5)))) {
        println(s"ERROR: Content-MD5 check failed for file $destPath.")
      }
    })
  }
}
在这个示例代码中，我们添加了Content-MD5检验功能，用于验证上传到S3兼容系统中的文件是否完整和正确。在检验之前，我们遍历了HDFS上的文件列表，计算出每个文件的MD5哈希值，并将其存储在一个Map中。然后，在执行DistCp上传操作后，我们遍历上传结果，验证每个文件的Content-MD5是否与预期的MD5哈希值一致。

需要注意的是，在验证Content-MD5时，我们使用了Hadoop提供的org.apache.hadoop.io.MD5Hash类来计算哈希值，而非Java原生的java.security.MessageDigest类。这是因为Hadoop的MD5哈希值与Java的



Eric H
请继续

Java原生的MD5哈希值格式不同，因此无法直接与Hadoop中的Content-MD5进行比较。而使用Hadoop提供的MD5Hash类，则可以保证哈希值的格式与Hadoop的Content-MD5格式一致，从而实现正确的比较和验证。

另外，需要注意的是，在计算MD5哈希值时，我们使用了缓冲输入流和分块读取的方式，以避免一次性读取大文件导致内存溢出的问题。具体来说，我们将文件分成大小为1MB的块，并使用一个长度为1MB的缓冲区来读取每个块，然后更新MD5哈希值。这样可以在保证计算准确性的同时，也能有效控制内存的使用。
