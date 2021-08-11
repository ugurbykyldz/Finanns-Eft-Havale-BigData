import com.mongodb.spark.MongoSpark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;

public class Application {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master" );
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder()
                .master("local").appName("EftHavale")
                .config("spark.mongodb.output.uri","mongodb://localhost/BankFinannsDB.eftHavale")
                .getOrCreate();

        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "eftHavale").load().selectExpr("CAST(value as STRING)");

        //loadDS.show();

        StructType accuntSchema = new StructType()
                .add("oid", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("iban", DataTypes.StringType);

        StructType infoSchema = new StructType()
                .add("name", DataTypes.StringType)
                .add("iban", DataTypes.StringType);


        StructType schema = new StructType()
                .add("pid", DataTypes.IntegerType)
                .add("pytpe", DataTypes.StringType)
                .add("account", accuntSchema)
                .add("info", infoSchema)
                .add("balance", DataTypes.IntegerType)
                .add("btype", DataTypes.StringType)
                .add("device", DataTypes.StringType)
                .add("bank", DataTypes.StringType)
                .add("city", DataTypes.StringType);

        Dataset<Row> rawDS = loadDS.select(functions.from_json(loadDS.col("value"), schema).as("data")).select("data.*");


        /*
        rawDS.show(10 , false);

        +-----+-----+-----------------------------------------------+-----------------------------------------+-------+-----+-----------------+----------+-------------+
        |pid  |pytpe|account                                        |info                                     |balance|btype|device           |bank      |city         |
        +-----+-----+-----------------------------------------------+-----------------------------------------+-------+-----+-----------------+----------+-------------+
        |10002|E    |[2660308, ÖMER FARUK ASLAN, TR1741338745585]   |[KAMERCAN SOYKAMER, TR1100997663623]     |1207   |TL   |Web aplication   |World Bank|Kastamonu    |
        |10003|H    |[3772077, HAVVA BÜKÜLMEZ, TR1964748092418]     |[DİLEK İMALI, TR1960092898904]           |276    |USD  |Mobile aplication|Happy Bank|Bartın       |
        |10004|H    |[1690363, ZÜBEYDE ÇOBAN, TR1119665893294]      |[MUHAMMED FURKAN BAŞER, TR1426412114668] |890    |EURO |Mobile aplication|Happy Bank|Kahramanmaraş|
        |10005|H    |[1337505, BURHANETTİN SÖNMEZ, TR1704949433751] |[REŞİT VOLKAN SARAÇOĞLU, TR1685139972652]|502    |TL   |Web aplication   |Water Bank|Sinop        |
        |10006|E    |[1894148, ELİF ARSLAN, TR1298120523156]        |[BAHADIR OSMANCA, TR1396739609110]       |104    |EURO |Mobile aplication|Happy Bank|Tokat        |
        |10007|E    |[3065049, CUNDULLAH ÜZEN, TR1082513158652]     |[AYLA DENİZ, TR1389003917388]            |670    |TL   |Mobile aplication|World Bank|Tekirdağ     |
        |10008|E    |[5125960, SALİHA DİLEK HALICI, TR1180367045858]|[MEHMET ORMAN, TR1802803542596]          |792    |TL   |Web aplication   |Happy Bank|Uşak         |
        |10009|E    |[5892989, ERAY YEŞİLKAYA, TR1369075642642]     |[SERHAT BURKAY YILMAZ, TR1727293755440]  |519    |TL   |Web aplication   |World Bank|Amasya       |
        |10010|E    |[89892, KADİR TOPAK, TR1146947029737]          |[MUSTAFA CİHAD KARASU, TR1241072754519]  |526    |EURO |Mobile aplication|Water Bank|Muğla        |
        |10011|E    |[4205574, HATUN ALTIN, TR1856893084055]        |[BURCU GÜRSOY, TR1238551623574]          |1446   |EURO |Web aplication   |Water Bank|Van          |
    +-----+-----+-----------------------------------------------+-----------------------------------------+-------+-----+-----------------+----------+-------------+

         */


      /*
        rawDS.printSchema();
        root
         |-- pid: integer (nullable = true)
         |-- pytpe: string (nullable = true)
         |-- account: struct (nullable = true)
         |    |-- oid: integer (nullable = true)
         |    |-- name: string (nullable = true)
         |    |-- iban: string (nullable = true)
         |-- info: struct (nullable = true)
         |    |-- name: string (nullable = true)
         |    |-- iban: string (nullable = true)
         |-- balance: integer (nullable = true)
         |-- btype: string (nullable = true)
         |-- device: string (nullable = true)
         |-- bank: string (nullable = true)
         |-- city: string (nullable = true)



         */

        /*
        rawDS.describe().show();
            +-------+------------------+-----+-----------------+-----+-----------------+----------+------+
            |summary|               pid|pytpe|          balance|btype|           device|      bank|  city|
            +-------+------------------+-----+-----------------+-----+-----------------+----------+------+
            |  count|             13866|13866|            13866|13866|            13866|     13866| 13866|
            |   mean|25220.721116399825| null|780.0162267416703| null|             null|      null|  null|
            | stddev|10908.594638778564| null|416.6101313455732| null|             null|      null|  null|
            |    min|             10002|    E|               50| EURO|Mobile aplication|Happy Bank| Adana|
            |    max|             49946|    H|             1500|  USD|   Web aplication|World Bank|Şırnak|
            +-------+------------------+-----+-----------------+-----+-----------------+----------+------+
         */

        //havale
        Dataset<Row> havale = rawDS.filter(rawDS.col("pytpe").equalTo("H"));
        /*
        havale.show(5, false);
        |pid  |pytpe|account                                       |info                                     |balance|btype|device           |bank      |city         |
        +-----+-----+----------------------------------------------+-----------------------------------------+-------+-----+-----------------+----------+-------------+
        |10003|H    |[3772077, HAVVA BÜKÜLMEZ, TR1964748092418]    |[DİLEK İMALI, TR1960092898904]           |276    |USD  |Mobile aplication|Happy Bank|Bartın       |
        |10004|H    |[1690363, ZÜBEYDE ÇOBAN, TR1119665893294]     |[MUHAMMED FURKAN BAŞER, TR1426412114668] |890    |EURO |Mobile aplication|Happy Bank|Kahramanmaraş|
        |10005|H    |[1337505, BURHANETTİN SÖNMEZ, TR1704949433751]|[REŞİT VOLKAN SARAÇOĞLU, TR1685139972652]|502    |TL   |Web aplication   |Water Bank|Sinop        |
        |10015|H    |[4157164, ÖZLEM KARAHAN, TR1580421959502]     |[HAMZA BÜYÜKPASTIRMACI, TR1765316457092] |182    |EURO |Mobile aplication|World Bank|Elazığ       |
        |10016|H    |[4329829, FUNDA KIYAK, TR1398946147170]       |[HASAN IŞIK, TR1175911797565]            |975    |TL   |Mobile aplication|Happy Bank|Diyarbakır   |
        +-----+-----+----------------------------------------------+-----------------------------------------+-------+-----+-----------------+----------+-------------+
         */

        //eft
        Dataset<Row> eft = rawDS.filter(rawDS.col("pytpe").equalTo("E"));
        /*

        eft.show(5, false);
        +-----+-----+-----------------------------------------------+---------------------------------------+-------+-----+-----------------+----------+---------+
        |pid  |pytpe|account                                        |info                                   |balance|btype|device           |bank      |city     |
        +-----+-----+-----------------------------------------------+---------------------------------------+-------+-----+-----------------+----------+---------+
        |10002|E    |[2660308, ÖMER FARUK ASLAN, TR1741338745585]   |[KAMERCAN SOYKAMER, TR1100997663623]   |1207   |TL   |Web aplication   |World Bank|Kastamonu|
        |10006|E    |[1894148, ELİF ARSLAN, TR1298120523156]        |[BAHADIR OSMANCA, TR1396739609110]     |104    |EURO |Mobile aplication|Happy Bank|Tokat    |
        |10007|E    |[3065049, CUNDULLAH ÜZEN, TR1082513158652]     |[AYLA DENİZ, TR1389003917388]          |670    |TL   |Mobile aplication|World Bank|Tekirdağ |
        |10008|E    |[5125960, SALİHA DİLEK HALICI, TR1180367045858]|[MEHMET ORMAN, TR1802803542596]        |792    |TL   |Web aplication   |Happy Bank|Uşak     |
        |10009|E    |[5892989, ERAY YEŞİLKAYA, TR1369075642642]     |[SERHAT BURKAY YILMAZ, TR1727293755440]|519    |TL   |Web aplication   |World Bank|Amasya   |
        +-----+-----+-----------------------------------------------+---------------------------------------+-------+-----+-----------------+----------+---------+
         */


        // havale.groupBy(rawDS.col("bank")).count().show(5, false);
        /*
        +----------+-----+
        |bank      |count|
        +----------+-----+
        |Happy Bank|1757 |
        |Love Bank |1641 |
        |World Bank|1739 |
        |Water Bank|1697 |
        +----------+-----+

         */

        // eft.groupBy(rawDS.col("bank")).count().show(5, false);
        /*
        +----------+-----+
        |bank      |count|
        +----------+-----+
        |Happy Bank|1687 |
        |Love Bank |1797 |
        |World Bank|1821 |
        |Water Bank|1727 |
        +----------+-----+
         */

        //bankların havale ve eft toplam ne kadar para akışı olmuş
        /*
        rawDS.groupBy("bank").pivot("pytpe").sum("balance").show(5 , false);


        +----------+-------+-------+
        |bank      |E      |H      |
        +----------+-------+-------+
        |Happy Bank|1296477|1383521|
        |Love Bank |1399948|1293449|
        |World Bank|1435588|1354788|
        |Water Bank|1330586|1321348|
        +----------+-------+-------+
         */



        //bankların havale ve eft toplam HANGİ para türüne ait  ne kadar para akışı olmuş
        /*
        rawDS.groupBy("bank","pytpe").pivot("btype").sum("balance").show(8, false);

        +----------+-----+------+------+------+
        |bank      |pytpe|EURO  |TL    |USD   |
        +----------+-----+------+------+------+
        |Water Bank|H    |417893|450511|452944|
        |Happy Bank|E    |441809|423057|431611|
        |Happy Bank|H    |441627|472088|469806|
        |Love Bank |H    |444173|422013|427263|
        |Love Bank |E    |455494|453462|490992|
        |World Bank|E    |486184|489568|459836|
        |Water Bank|E    |412510|458275|459801|
        |World Bank|H    |456979|482411|415398|
        +----------+-----+------+------+------+
         */

        //bankların havale ve eft toplam HANGİ para türüne ait, hangi application kullanarak  ne kadar para akışı olmuş
        Dataset<Row> bankDevicePytpe = rawDS.groupBy("bank", "pytpe", "device").pivot("btype").sum("balance");

        /*
        bankDevicePytpe.show(10, false);
        +----------+-----+-----------------+------+------+------+
        |bank      |pytpe|device           |EURO  |TL    |USD   |
        +----------+-----+-----------------+------+------+------+
        |Happy Bank|E    |Mobile aplication|227526|209139|196257|
        |Love Bank |E    |Web aplication   |216897|221250|251286|
        |Happy Bank|E    |Web aplication   |214283|213918|235354|
        |Love Bank |H    |Mobile aplication|203288|209743|210086|
        |Water Bank|H    |Web aplication   |203967|222299|211487|
        |Happy Bank|H    |Mobile aplication|215905|220098|246574|
        |Love Bank |E    |Mobile aplication|238597|232212|239706|
        |World Bank|H    |Mobile aplication|240093|230366|194598|
        |Water Bank|E    |Web aplication   |202119|207199|228262|
        |World Bank|H    |Web aplication   |216886|252045|220800|
        +----------+-----+-----------------+------+------+------+
         */


        //mongoDB kaydetme

       MongoSpark.write(bankDevicePytpe).option("collection", "BankDevicePytype").mode("overwrite").save();

    }
}

