package com.bigdata.spark.sparkDataFrame

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JsonProcessPractice {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JsonProcessPractice").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data="file:///D:\\bigdata\\datasets\\companies.json"
    val df=spark.read.format("json").load(data)
   // df.printSchema()

    val res=df.withColumn("acquisitions",explode($"acquisitions"))
      .withColumn("competitions",explode($"competitions"))
      .withColumn("external_links",explode($"external_links"))
      .withColumn("funding_rounds",explode($"funding_rounds"))
      .withColumn("investments",explode($"investments"))
      .withColumn("milestones",explode($"milestones"))
      .withColumn("offices",explode($"offices"))
      .withColumn("partners",explode($"partners"))
      .withColumn("products",explode($"products"))
      .withColumn("providerships",explode($"providerships"))
      .withColumn("relationships",explode($"relationships"))
      .withColumn("screenshots",explode($"screenshots"))
      .withColumn("video_embeds",explode($"video_embeds"))
      .withColumn("idoid",$"_id.$$oid")
    //res.printSchema()

   val res1=res.select($"*",$"ipo.*",$"video_embeds.description".alias("video_embeds_description"),$"video_embeds.embed_code".alias("video_embeds_embed_code")
   ,$"products.name".alias("products_name"),$"products.permalink".alias("products_permalink")
    ,$"external_links.external_url".alias("external_links_url"),$"external_links.title".alias("external_links_title")
     ,$"competitions.competitor.name".alias("competn.comptitor_name"),$"competitions.competitor.permalink".alias("competn.comptitor.plink")
     ,$"acquisition.acquired_day".alias("acquisition.acquired_day"),$"acquisition.acquired_month".alias("acquisition.acquired_month")
     ,$"acquisition.acquired_year".alias("acquisition.acquired_year"),$"acquisition.acquiring_company.name".alias("acquisition.acquiring_company.name")
     ,$"acquisition.price_amount".alias("acquisition.price_amount"),$"acquisition.price_currency_code".alias("acquisition.price_currency_code")
     ,$"acquisition.source_description".alias("acquisition.source_description")
     ,$"acquisition.source_url".alias("acquisition.source_url")
    ,$"acquisition.term_code".alias("acquisition.term_code"),$"acquisition.acquiring_company.permalink".alias("acquisition.acquiring_company.permalink")
     ,$"acquisitions.acquired_day".alias("acquisitions.acquired_day"),$"acquisitions.acquired_month".alias("acquisitions.acquired_month")
    ,$"acquisitions.acquired_year".alias("acquisitions.acquired_year"),$"acquisitions.company.name".alias("acquisitions.company.name")
    ,$"acquisitions.price_amount".alias("acquisitions.price_amount"),$"acquisitions.price_currency_code".alias("acquisitions.price_currency_code")
    ,$"acquisitions.source_description".alias("acquisitions.source_description"),$"acquisitions.source_url".alias("acquisitions.source_url")
    ,$"acquisitions.term_code".alias("acquisitions.term_code"),$"acquisitions.company.permalink".alias("acquisitions.company.permalink")
    ,$"partners.homepage_url".alias("partners.homepage_url"),$"partners.link_1_name".alias("partners.link_1_name")
    ,$"partners.link_1_url".alias("partners.link_1_url"),$"partners.link_2_name".alias("partners.link_2_name")
    ,$"partners.link_2_url".alias("partners.link_2_url"),$"partners.link_3_name".alias("partners.link_3_name")
    ,$"partners.link_3_url".alias("partners.link_3_url"),$"partners.partner_name".alias("partners.partner_name")
    ,$"providerships.is_past".alias("providerships.is_past"),$"providerships.provider.permalink".alias("providerships.provider.permalink")
    ,$"providerships.title".alias("providerships.title"),$"providerships.provider.name".alias("providerships.provider.name")
    ,$"relationships.is_past".alias("relationships.is_past"),$"relationships.person.first_name".alias("relationships.person.first_name")
    ,$"relationships.person.last_name".alias("relationships.person.last_name"),$"relationships.person.permalink".alias("relationships.person.permalink")
    ,$"relationships.title".alias("relationships.title")
     ,$"offices.address1".alias("offices.address1"),$"offices.address2".alias("offices.address2")
     ,$"offices.city".alias("offices.city"),$"offices.country_code".alias("offices.country_code")
     ,$"offices.description".alias("offices.description"),$"offices.latitude".alias("offices.latitude")
     ,$"offices.longitude".alias("offices.longitude"),$"offices.state_code".alias("offices.state_code"),$"offices.zip_code".alias("offices.zip_code")
     ,$"investments.funding_round.company.name".alias("investments.funding_round.company.name"),$"investments.funding_round.company.permalink".alias("investments.funding_round.company.permalink")
     ,$"investments.funding_round.funded_day".alias("investments.funding_round.funded_day"),$"investments.funding_round.funded_month".alias("investments.funding_round.funded_month")
     ,$"investments.funding_round.funded_year".alias("investments.funding_round.funded_year"),$"investments.funding_round.raised_amount".alias("investments.funding_round.raised_amount")
     ,$"investments.funding_round.raised_currency_code".alias("investments.funding_round.raised_currency_code"),$"investments.funding_round.round_code".alias("investments.funding_round.round_code")
     ,$"investments.funding_round.source_description".alias("investments.funding_round.source_description"),$"investments.funding_round.source_url".alias("investments.funding_round.source_url")
     ,$"milestones.description".alias("milestones.description"),$"milestones.id".alias("milestones.id")
     ,$"milestones.source_description".alias("milestones.source_description"),$"milestones.stoneable_type".alias("milestones.stoneable_type")
     ,$"milestones.source_text".alias("milestones.source_text"),$"milestones.source_url".alias("milestones.source_url")
     ,$"milestones.stoneable.name".alias("milestones.stoneable.name"),$"milestones.stoneable.permalink".alias("milestones.stoneable.permalink")
     ,$"milestones.stoned_acquirer".alias("milestones.stoned_acquirer"),$"milestones.stoned_day".alias("milestones.stoned_day")
     ,$"milestones.stoned_month".alias("milestones.stoned_month"),$"milestones.stoned_value".alias("milestones.stoned_value")
     ,$"milestones.stoned_value_type".alias("milestones.stoned_value_type"),$"milestones.stoned_year".alias("milestones.stoned_year")
     )
     .drop("video_embeds","_id","products","external_links","competitions","acquisition","acquisitions","ipo","partners","providerships","relationships","offices","investments","milestones")

    res1.printSchema()

    spark.stop()
  }
}
/*
  ,$"funding_rounds.funded_day".alias("funding_rounds.funded_day"),$"funding_rounds.funded_month".alias("funding_rounds.funded_month")
	,$"funding_rounds.funded_year".alias("funding_rounds.funded_year"),$"funding_rounds.id".alias("funding_rounds.id")
    ,$"funding_rounds.raised_amount".alias("funding_rounds.raised_amount"),$"funding_rounds.raised_currency_code".alias("funding_rounds.raised_currency_code")
    ,$"funding_rounds.round_code".alias("funding_rounds.round_code"),$"funding_rounds.source_description".alias("funding_rounds.source_description")
	,$"funding_rounds.source_url".alias("funding_rounds.source_url")

 */