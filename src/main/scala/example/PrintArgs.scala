package example

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{MetadataBuilder, StructField}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PrintArgs {

  private val sas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-31T11:59:38Z&st=2023-11-28T03:59:38Z&spr=https&sig=KkIG3WCIyi%2FzQ%2BfG1UZu208%2FDlBQcK0EARViPkNo0vk%3D"
  private val myFileMountPoint = "/mnt/myfilevhl"
  private val expectedFileMountPoint = "/mnt/myexpectvhl"
  private val outputFileMountPoint = "/mnt/outputvhl"
  private val containerLv1 = "level1"
  private val containerLv2 = "level2"
  private val containerLv3 = "level3"
  private val parquetFile = "userdata5.parquet"
  private val expectedParquetFile = "userdata1.parquet"
  private val columnNameNeedBeChanged = "cc"
  private val newColumnName = "cc_mod"
  private val storageAccountName = "storagedesharing"
  private val keyVaultSecretScope = "secretScopeDESharing"
  private val storageAccountAccessKey = "7d6dLH8DQdWseDoCbFc9QgMaGwiWylh19UekbKXnImBbGt9IdP3cBhqF6SApKsoJ9Aj3Ya01XbEc+AStnKgPIQ=="

  private def unmountAllFile(): Unit = {
    println("Unmount file for next testing")
    dbutils.fs.unmount(myFileMountPoint)
    dbutils.fs.unmount(expectedFileMountPoint)
    dbutils.fs.unmount(outputFileMountPoint)
  }

  private def getKeyVaultSecret(keyName: String): String = {
    dbutils.secrets.get(scope = keyVaultSecretScope, key = keyName)
  }

  private def transformToDWTable(dataFrame: DataFrame, tempDir: String, dwDatabase: String,
                                 dwServer: String, dwUser: String, dwPass: String): Unit = {
    val dwJdbcPort = "1433"
    val sqlDWUrlFooter = "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
    val sqlDwUrl = s"jdbc:sqlserver://$dwServer:$dwJdbcPort;database=$dwDatabase;user=$dwUser;password=$dwPass;$sqlDWUrlFooter"
    dataFrame.write
      .format("com.databricks.spark.sqldw")
      .option("url", sqlDwUrl)
      .option("dbTable", "UserTable")
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("tempDir", tempDir)
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def getStringConfig(containerName: String): String = {
    "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
  }

  private def isAllMatch(array1: Array[StructField], array2: Array[StructField]): Boolean = {
    val s1 = array1.map(e => s"${e.name} ${e.dataType}")
    val s2 = array2.map(e => s"${e.name} ${e.dataType}")
    s1.sameElements(s2) && array1.length == array2.length
  }

  private def getDir(containerName: String, blobStorage: String
  = storageAccountName) = s"wasbs://$containerName@$blobStorage.blob.core.windows.net/"

  private def dataBrickMount(containerName: String, fileName: String = "", outputMountPoint: String): Unit = {
    dbutils.fs.mount(
      source = s"${getDir(containerName)}$fileName",
      mountPoint = outputMountPoint,
      extraConfigs = Map(getStringConfig(containerName) -> sas))
  }

  private def jobBody(): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    dataBrickMount(containerLv2, parquetFile, myFileMountPoint)

    println("2.a Read data from level2 container")
    val mydf = spark.read.parquet(myFileMountPoint)
    mydf.printSchema()

    println("2.b: Validate if real data schema in the parquet files match Expected schema.")
    dataBrickMount(containerLv1, expectedParquetFile, expectedFileMountPoint)
    val expecteddf = spark.read.parquet(expectedFileMountPoint)
    val efields = expecteddf.schema.fields
    val cfields = mydf.schema.fields

    val test = isAllMatch(cfields, efields)
    println(if (test) "Match" else "Unmatch")

    println("2.c: Rename column cc to cc_mod")
    val newDF: DataFrame = mydf.withColumnRenamed(columnNameNeedBeChanged, newColumnName)

    println("2.d: Add a tag “this column has been modified” to metadata of cc_mod column")
    val metadataBuilder = new MetadataBuilder().putString("tag", "this column has been modified").build()
    val newColumnValue = newDF.col(newColumnName).as(newColumnName, metadataBuilder)
    newDF.withColumn(newColumnName, newColumnValue)
      .schema.fields.foreach(c => println(s"${c.name} - metadata: ${c.metadata}"))

    println("2.e: Remove duplicated rows")
    println(s"Before: ${newDF.count()}")
    val after = newDF.distinct().count()
    println(s"After: $after")

    println("Starting - Transformed data to parquet file to level3 container")
    dataBrickMount(containerLv3, outputMountPoint = outputFileMountPoint)
    newDF.write.mode(SaveMode.Overwrite).parquet(outputFileMountPoint)
    println("Ending - Transformed data to parquet file to level3 container")

    println("Starting - Transformed data to parquet file to a table in Data Warehouse")
    val sc = SparkContext.getOrCreate()
    sc.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.dfs.core.windows.net", storageAccountAccessKey)
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    val tempDir = s"abfss://$containerLv3@$storageAccountName.dfs.core.windows.net/tempFolder"
    val dwServer = "demosynap.sql.azuresynapse.net"
    val dwDatabase = "dedicatedSQLpool"
    val dwUser = getKeyVaultSecret("DatabaseAccount")
    val dwPass = getKeyVaultSecret("ADSLLouis")
    transformToDWTable(newDF, tempDir, dwDatabase, dwServer, dwUser, dwPass)
    println("Ending - Transformed data to parquet file to a table in Data Warehouse")

  }

  private def jobCleanup(): Unit = {
    unmountAllFile()
  }

  def main(args: Array[String]): Unit = {
    try {
      jobBody()
    } finally {
      jobCleanup()
    }
  }
}
