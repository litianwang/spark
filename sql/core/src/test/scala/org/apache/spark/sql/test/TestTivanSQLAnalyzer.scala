/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.test

import java.util.concurrent.Callable

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{AnalysisException, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry, SimpleFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, _}
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SchemaUtils



object TestTivanSQLAnalyzer {

  def main(args: Array[String]): Unit = {

    sqlAnalyzer(
      """
        |select *
        |from test1
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select *
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select thedate, dtEventTime, f_int, f_double
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    sqlAnalyzer(
      """
        |select thedate, dtEventTime, f_int, f_double
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    // agg
    sqlAnalyzer(
      """
        |select
        |count(*) as cnt1,
        |count(1) as cnt2,
        |sum(f_int) as sum_i,
        |sum(f_long) as sum_l,
        |sum(f_float) as sum_f,
        |sum(f_double) as sum_d,
        |avg(f_long) as avg_l,
        |max(f_long) as max_l,
        |min(f_long) as min_l
        |from test1
        |where thedate='20190611'
      """.stripMargin
    )

    // join
    sqlAnalyzer(
      """
        |select t1.*, t2.* from test1 t1, test2 t2 where t1.dtEventTimeStamp = t2.dtEventTimeStamp
      """.stripMargin
    )

    // join
    sqlAnalyzer(
      """
        |select t1.f_int, t1.f_long, t2.thedate, t2.localTime from test1 t1 left join test2 t2 on (t1.dtEventTimeStamp = t2.dtEventTimeStamp)
      """.stripMargin
    )

    // union
    sqlAnalyzer(
      """
        |select * from test1
        |union
        |select * from test2
      """.stripMargin
    )
    // union all
    sqlAnalyzer(
      """
        |select * from test1
        |union all
        |select * from test2
      """.stripMargin
    )

    // sub query
    sqlAnalyzer(
      """
        |select
        | thedate,
        | count(*) as cnt,
        | sum(f_int) as sum_i
        | from (
        |select t1.f_int, t1.f_long, t2.thedate, t2.localTime from test1 t1 left join test2 t2 on (t1.dtEventTimeStamp = t2.dtEventTimeStamp)
        |  )
        | group by  thedate
      """.stripMargin
    )

    // grouping sets
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C grouping sets((A,B),(A,C))
      """.stripMargin
    )

    // cube
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C with cube
      """.stripMargin
    )


    // rollup
    sqlAnalyzer(
      """
        |select
        | A,B,C,count(*) as cnt
        | from test4
        | group by A,B,C with rollup
      """.stripMargin
    )

    // rollup GROUPING__ID
    sqlAnalyzer(
      """
        |select
        | A,B,C,
        | GROUPING__ID,
        | count(*) as cnt
        | from test4
        | group by A,B,C with rollup
      """.stripMargin
    )

    // window function
    sqlAnalyzer(
      """
        |select f.udid,
        |       f.from_id,
        |       f.ins_date
        |from
        |  (select /* +MAPJOIN(u) */ u.device_id as udid,
        |                            g.device_id as gdid,
        |                            u.from_id,
        |                            u.ins_date,
        |                            row_number() over (partition by u.device_id
        |                                               order by u.ins_date asc) as row_number
        |   from user_device_info u
        |   left outer join
        |     (select device_id
        |      from 3g_device_id
        |      where log_date<'2013-07-25') g on (u.device_id = g.device_id)
        |   where u.log_date='2013-07-25'
        |     and u.device_id is not null
        |     and u.device_id <> '') f
        |where f.gdid is null
        |  and row_number=1
      """.stripMargin
    )

    // sub query
    sqlAnalyzer(
      """
        |select (select (select 1) + 1) + 1 as s1
      """.stripMargin)

    // with
    sqlAnalyzer(
      """
        |with t2 as (with t1 as (select 1 as b, 2 as c) select b, c from t1)
        | select a from (select 1 as a union all select 2 as a) t
        | where a = (select max(b) from t2)
      """.stripMargin)

    // with
    sqlAnalyzer(
      """
        |with t2 as (select 1 as b, 2 as c)
        | select a from (select 1 as a union all select 2 as a) t
        | where a = (select max(b) from t2)
      """.stripMargin)


  }

   def getTables(): Seq[CatalogTable] = {
     val tables = new ListBuffer[CatalogTable]()
     tables.append(createTableDesc(
       "test1",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )
     tables.append(createTableDesc(
       "test2",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )
     tables.append(createTableDesc(
       "test3",
       new StructType()
         .add("thedate", StringType)
         .add("dtEventTime", StringType)
         .add("dtEventTimeStamp", LongType)
         .add("localTime", StringType)
         .add("f_int", IntegerType)
         .add("f_long", LongType)
         .add("f_float", FloatType)
         .add("f_double", DoubleType)
     )
     )

     tables.append(createTableDesc(
       "test4",
       new StructType()
         .add("a", StringType)
         .add("b", StringType)
         .add("c", LongType)
         .add("d", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )

     tables.append(createTableDesc(
       "user_device_info",
       new StructType()
         .add("device_id", StringType)
         .add("from_id", StringType)
         .add("ins_date", LongType)
         .add("log_date", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )


     tables.append(createTableDesc(
       "3g_device_id",
       new StructType()
         .add("device_id", StringType)
         .add("from_id", StringType)
         .add("ins_date", LongType)
         .add("log_date", StringType)
         .add("e", IntegerType)
         .add("f", LongType)
         .add("g", FloatType)
         .add("h", DoubleType)
     )
     )

     tables.toSeq
   }

  private def createTableDesc(tableName: String, schema: StructType): CatalogTable = {
    val tableDesc = CatalogTable(
      identifier = TableIdentifier(tableName, Some(SessionCatalog.DEFAULT_DATABASE)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = schema,
      provider = Some("parquet"),
      createTime = 0L,
      createVersion = org.apache.spark.SPARK_VERSION
    )
    tableDesc
  }

  private def sqlAnalyzer(sql: String, tables: Seq[CatalogTable] = getTables()): Unit = {
    val qe = parserSQL(sql, tables)
    // scalastyle:off println
    println(qe.schema.fields.mkString(", "))
    qe.printSchema()
  }

  private def parserSQL(sql: String, tables: Seq[CatalogTable] = Seq.empty): LogicalPlan = {
    val conf = new SQLConf()
    val sqlParser = new SparkSqlParser(conf)
    val functionRegistry = getFunctionRegistry()
    val sessionCatalog = createMemorySessionCatalog(conf, functionRegistry)
    tables.foreach( f => sessionCatalog.createTable(f, true))
    val lPlan: LogicalPlan = sqlParser.parsePlan(sql);
    val analyzer = createAnalyzer(conf, sessionCatalog)
    val qe = analyzer.executeAndCheck(lPlan)
    qe
  }

  private def getFunctionRegistry(): FunctionRegistry = {
    val functionRegistry: FunctionRegistry = new SimpleFunctionRegistry()
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
    functionRegistry
  }

  private def createAnalyzer(conf: SQLConf, sessionCatalog: SessionCatalog): Analyzer = {
    new Analyzer(sessionCatalog, new SQLConf()) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
          new MyFindDataSourceTable(sessionCatalog) +:
          Nil
      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
          MyPreProcessTableCreation(conf, sessionCatalog) +:
          PreprocessTableInsertion(conf) +:
          DataSourceAnalysis(conf) +:
          Nil
      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
        PreWriteCheck +:
          PreReadCheck +:
          HiveOnlyCheck +:
          Nil
    }
  }


  private def createMemorySessionCatalog(conf: SQLConf,
                                 functionRegistry: FunctionRegistry): SessionCatalog = {

    val defaultDbDefinition = CatalogDatabase(
      SessionCatalog.DEFAULT_DATABASE,
      "default",
      CatalogUtils.stringToURI("/tmp/tivanli"),
      Map())
    val catalog = new InMemoryCatalog()
    catalog.createDatabase(defaultDbDefinition, true)
    // catalog.createTable(tableDesc, true);
    val sessionCatalog = new SessionCatalog(catalog, functionRegistry, conf)
    sessionCatalog
  }
}

//////////////////////////////////////////////////////////////////////////////////////////

case class MyBaseRelation(userSpecifiedSchema: StructType)
  extends BaseRelation {

  override def sqlContext: SQLContext = null

  override def sizeInBytes: Long = Long.MaxValue

  override def schema: StructType = userSpecifiedSchema

}

case class MyPreProcessTableCreation(conf1: SQLConf,
                                     catalog1: SessionCatalog) extends Rule[LogicalPlan] {
  // catalog is a def and not a val/lazy val as the latter would introduce a circular reference
  private def catalog = catalog1

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // When we CREATE TABLE without specifying the table schema, we should fail the query if
    // bucketing information is specified, as we can't infer bucketing from data files currently.
    // Since the runtime inferred partition columns could be different from what user specified,
    // we fail the query if the partitioning information is specified.
    case c @ CreateTable(tableDesc, _, None) if tableDesc.schema.isEmpty =>
      if (tableDesc.bucketSpec.isDefined) {
        failAnalysis("Cannot specify bucketing information if the table schema is not specified " +
          "when creating and will be inferred at runtime")
      }
      if (tableDesc.partitionColumnNames.nonEmpty) {
        failAnalysis("It is not allowed to specify partition columns when the table schema is " +
          "not defined. When the table schema is not provided, schema and partition columns " +
          "will be inferred.")
      }
      c

    // When we append data to an existing table, check if the given provider, partition columns,
    // bucket spec, etc. match the existing table, and adjust the columns order of the given query
    // if necessary.
    case c @ CreateTable(tableDesc, SaveMode.Append, Some(query))
      if query.resolved && catalog.tableExists(tableDesc.identifier) =>
      // This is guaranteed by the parser and `DataFrameWriter`
      assert(tableDesc.provider.isDefined)

      val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
      val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
      val tableName = tableIdentWithDB.unquotedString
      val existingTable = catalog.getTableMetadata(tableIdentWithDB)

      if (existingTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException("Saving data into a view is not allowed.")
      }

      // Check if the specified data source match the data source of the existing table.
      val conf = conf1
      val existingProvider = DataSource.lookupDataSource(existingTable.provider.get, conf)
      val specifiedProvider = DataSource.lookupDataSource(tableDesc.provider.get, conf)
      // TODO: Check that options from the resolved relation match the relation that we are
      // inserting into (i.e. using the same compression).
      if (existingProvider != specifiedProvider) {
        throw new AnalysisException(s"The format of the existing table $tableName is " +
          s"`${existingProvider.getSimpleName}`. It doesn't match the specified format " +
          s"`${specifiedProvider.getSimpleName}`.")
      }
      tableDesc.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          throw new AnalysisException(
            s"The location of the existing table ${tableIdentWithDB.quotedString} is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${tableDesc.location}`.")
        case _ =>
      }

      if (query.schema.length != existingTable.schema.length) {
        throw new AnalysisException(
          s"The column number of the existing table $tableName" +
            s"(${existingTable.schema.catalogString}) doesn't match the data schema" +
            s"(${query.schema.catalogString})")
      }

      val resolver = conf1.resolver
      val tableCols = existingTable.schema.map(_.name)

      // As we are inserting into an existing table, we should respect the existing schema and
      // adjust the column order of the given dataframe according to it, or throw exception
      // if the column names do not match.
      val adjustedColumns = tableCols.map { col =>
        query.resolve(Seq(col), resolver).getOrElse {
          val inputColumns = query.schema.map(_.name).mkString(", ")
          throw new AnalysisException(
            s"cannot resolve '$col' given input columns: [$inputColumns]")
        }
      }

      // Check if the specified partition columns match the existing table.
      val specifiedPartCols = CatalogUtils.normalizePartCols(
        tableName, tableCols, tableDesc.partitionColumnNames, resolver)
      if (specifiedPartCols != existingTable.partitionColumnNames) {
        val existingPartCols = existingTable.partitionColumnNames.mkString(", ")
        throw new AnalysisException(
          s"""
             |Specified partitioning does not match that of the existing table $tableName.
             |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
             |Existing partition columns: [$existingPartCols]
          """.stripMargin)
      }

      // Check if the specified bucketing match the existing table.
      val specifiedBucketSpec = tableDesc.bucketSpec.map { bucketSpec =>
        CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
      }
      if (specifiedBucketSpec != existingTable.bucketSpec) {
        val specifiedBucketString =
          specifiedBucketSpec.map(_.toString).getOrElse("not bucketed")
        val existingBucketString =
          existingTable.bucketSpec.map(_.toString).getOrElse("not bucketed")
        throw new AnalysisException(
          s"""
             |Specified bucketing does not match that of the existing table $tableName.
             |Specified bucketing: $specifiedBucketString
             |Existing bucketing: $existingBucketString
          """.stripMargin)
      }

      val newQuery = if (adjustedColumns != query.output) {
        Project(adjustedColumns, query)
      } else {
        query
      }

      c.copy(
        tableDesc = existingTable,
        query = Some(DDLPreprocessingUtils.castAndRenameQueryOutput(
          newQuery, existingTable.schema.toAttributes, conf)))

    // Here we normalize partition, bucket and sort column names, w.r.t. the case sensitivity
    // config, and do various checks:
    //   * column names in table definition can't be duplicated.
    //   * partition, bucket and sort column names must exist in table definition.
    //   * partition, bucket and sort column names can't be duplicated.
    //   * can't use all table columns as partition columns.
    //   * partition columns' type must be AtomicType.
    //   * sort columns' type must be orderable.
    //   * reorder table schema or output of query plan, to put partition columns at the end.
    case c @ CreateTable(tableDesc, _, query) if query.forall(_.resolved) =>
      if (query.isDefined) {
        assert(tableDesc.schema.isEmpty,
          "Schema may not be specified in a Create Table As Select (CTAS) statement")

        val analyzedQuery = query.get
        val normalizedTable = normalizeCatalogTable(analyzedQuery.schema, tableDesc)

        val output = analyzedQuery.output
        val partitionAttrs = normalizedTable.partitionColumnNames.map { partCol =>
          output.find(_.name == partCol).get
        }
        val newOutput = output.filterNot(partitionAttrs.contains) ++ partitionAttrs
        val reorderedQuery = if (newOutput == output) {
          analyzedQuery
        } else {
          Project(newOutput, analyzedQuery)
        }

        c.copy(tableDesc = normalizedTable, query = Some(reorderedQuery))
      } else {
        val normalizedTable = normalizeCatalogTable(tableDesc.schema, tableDesc)

        val partitionSchema = normalizedTable.partitionColumnNames.map { partCol =>
          normalizedTable.schema.find(_.name == partCol).get
        }

        val reorderedSchema =
          StructType(normalizedTable.schema.filterNot(partitionSchema.contains) ++ partitionSchema)

        c.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
      }
  }

  private def normalizeCatalogTable(schema: StructType, table: CatalogTable): CatalogTable = {
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema,
      "in the table definition of " + table.identifier,
      conf1.caseSensitiveAnalysis)

    val normalizedPartCols = normalizePartitionColumns(schema, table)
    val normalizedBucketSpec = normalizeBucketSpec(schema, table)

    normalizedBucketSpec.foreach { spec =>
      for (bucketCol <- spec.bucketColumnNames if normalizedPartCols.contains(bucketCol)) {
        throw new AnalysisException(s"bucketing column '$bucketCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
      for (sortCol <- spec.sortColumnNames if normalizedPartCols.contains(sortCol)) {
        throw new AnalysisException(s"bucket sorting column '$sortCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
    }

    table.copy(partitionColumnNames = normalizedPartCols, bucketSpec = normalizedBucketSpec)
  }

  private def normalizePartitionColumns(schema: StructType, table: CatalogTable): Seq[String] = {
    val normalizedPartitionCols = CatalogUtils.normalizePartCols(
      tableName = table.identifier.unquotedString,
      tableCols = schema.map(_.name),
      partCols = table.partitionColumnNames,
      resolver = conf1.resolver)

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartitionCols,
      "in the partition schema",
      conf1.resolver)

    if (schema.nonEmpty && normalizedPartitionCols.length == schema.length) {
      if (DDLUtils.isHiveTable(table)) {
        // When we hit this branch, it means users didn't specify schema for the table to be
        // created, as we always include partition columns in table schema for hive serde tables.
        // The real schema will be inferred at hive metastore by hive serde, plus the given
        // partition columns, so we should not fail the analysis here.
      } else {
        failAnalysis("Cannot use all columns for partition columns")
      }

    }

    schema.filter(f => normalizedPartitionCols.contains(f.name)).map(_.dataType).foreach {
      case _: AtomicType => // OK
      case other => failAnalysis(s"Cannot use ${other.simpleString} for partition column")
    }

    normalizedPartitionCols
  }

  private def normalizeBucketSpec(schema: StructType, table: CatalogTable): Option[BucketSpec] = {
    table.bucketSpec match {
      case Some(bucketSpec) =>
        val normalizedBucketSpec = CatalogUtils.normalizeBucketSpec(
          tableName = table.identifier.unquotedString,
          tableCols = schema.map(_.name),
          bucketSpec = bucketSpec,
          resolver = conf1.resolver)

        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.bucketColumnNames,
          "in the bucket definition",
          conf1.resolver)
        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.sortColumnNames,
          "in the sort definition",
          conf1.resolver)

        normalizedBucketSpec.sortColumnNames.map(schema(_)).map(_.dataType).foreach {
          case dt if RowOrdering.isOrderable(dt) => // OK
          case other => failAnalysis(s"Cannot use ${other.simpleString} for sorting column")
        }

        Some(normalizedBucketSpec)

      case None => None
    }
  }

  private def failAnalysis(msg: String) = throw new AnalysisException(msg)
}



class MyFindDataSourceTable(catalog1: SessionCatalog) extends Rule[LogicalPlan] {
  private def readDataSourceTable(table: CatalogTable): LogicalPlan = {
    val qualifiedTableName = QualifiedTableName(table.database, table.identifier.table)
    val catalog = catalog1
    catalog.getCachedPlan(qualifiedTableName, new Callable[LogicalPlan]() {
      override def call(): LogicalPlan = {
        LogicalRelation(new MyBaseRelation(table.schema), table)
      }
    })
  }

  private def readHiveTable(table: CatalogTable): LogicalPlan = {
    HiveTableRelation(
      table,
      // Hive table columns are always nullable.
      table.dataSchema.asNullable.toAttributes,
      table.partitionSchema.asNullable.toAttributes)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _)
      if DDLUtils.isDatasourceTable(tableMeta) =>
      i.copy(table = readDataSourceTable(tableMeta))

    case i @ InsertIntoTable(UnresolvedCatalogRelation(tableMeta), _, _, _, _) =>
      i.copy(table = readHiveTable(tableMeta))

    case UnresolvedCatalogRelation(tableMeta) if DDLUtils.isDatasourceTable(tableMeta) =>
      readDataSourceTable(tableMeta)

    case UnresolvedCatalogRelation(tableMeta) =>
      readHiveTable(tableMeta)
  }
}
