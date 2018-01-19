

```scala
val twitterPath="cache-0-json"
```


```scala
import play.api.libs.json._
```


```scala
val twitterData=sc.textFile(twitterPath).map(x => Json.parse(x))
```


```scala
twitterData.first()\"text"
```




   "Obama vies for health care edge in Florida - http://t.co/OcISvreb http://t.co/FsJ7xgGW #florida"




```scala
twitterData.filter(x => (x\"user"\"screen_name").toString.contains("realDonaldTrump")).map(x => x\"text").take(10)
```




   Array("Obama asked a 7 yr old for his birth certificate. He's \"in your face\" because the Republicans dropped the ball. (cont) http://t.co/FufZD79U", "Obama is taunting the Republicans on the birther issue. They should call his bluff &amp; demand the REAL facts. He (cont) http://t.co/NWmVp06e", "\"President Obama is the greatest hoax ever perpetrated on the American people\"\n--Clint Eastwood")




```scala
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
```


```scala
val sqlC= new SQLContext(sc) 
```


```scala
val twitterTable=sqlC.read.json(twitterPath)
```


```scala
twitterTable.registerTempTable("twitterTable")
```


```scala
twitterTable.getClass
```




   class org.apache.spark.sql.DataFrame




```scala
twitterTable.printSchema()
```

    root
     |-- contributors: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- coordinates: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- created_at: string (nullable = true)
     |-- entities: struct (nullable = true)
     |    |-- hashtags: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- urls: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- user_mentions: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- screen_name: string (nullable = true)
     |-- favorited: boolean (nullable = true)
     |-- geo: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- id: long (nullable = true)
     |-- id_str: string (nullable = true)
     |-- in_reply_to_screen_name: string (nullable = true)
     |-- in_reply_to_status_id: long (nullable = true)
     |-- in_reply_to_status_id_str: string (nullable = true)
     |-- in_reply_to_user_id: long (nullable = true)
     |-- in_reply_to_user_id_str: string (nullable = true)
     |-- place: struct (nullable = true)
     |    |-- attributes: struct (nullable = true)
     |    |    |-- locality: string (nullable = true)
     |    |    |-- street_address: string (nullable = true)
     |    |-- bounding_box: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- country: string (nullable = true)
     |    |-- country_code: string (nullable = true)
     |    |-- full_name: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- place_type: string (nullable = true)
     |    |-- url: string (nullable = true)
     |-- possibly_sensitive: boolean (nullable = true)
     |-- possibly_sensitive_editable: boolean (nullable = true)
     |-- retweet_count: long (nullable = true)
     |-- retweeted: boolean (nullable = true)
     |-- retweeted_status: struct (nullable = true)
     |    |-- contributors: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- coordinates: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- favorited: boolean (nullable = true)
     |    |-- geo: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- in_reply_to_screen_name: string (nullable = true)
     |    |-- in_reply_to_status_id: long (nullable = true)
     |    |-- in_reply_to_status_id_str: string (nullable = true)
     |    |-- in_reply_to_user_id: long (nullable = true)
     |    |-- in_reply_to_user_id_str: string (nullable = true)
     |    |-- place: struct (nullable = true)
     |    |    |-- attributes: struct (nullable = true)
     |    |    |    |-- street_address: string (nullable = true)
     |    |    |-- bounding_box: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- country: string (nullable = true)
     |    |    |-- country_code: string (nullable = true)
     |    |    |-- full_name: string (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- place_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |-- possibly_sensitive: boolean (nullable = true)
     |    |-- possibly_sensitive_editable: boolean (nullable = true)
     |    |-- retweet_count: long (nullable = true)
     |    |-- retweeted: boolean (nullable = true)
     |    |-- scopes: struct (nullable = true)
     |    |    |-- followers: boolean (nullable = true)
     |    |-- source: string (nullable = true)
     |    |-- text: string (nullable = true)
     |    |-- truncated: boolean (nullable = true)
     |    |-- user: struct (nullable = true)
     |    |    |-- contributors_enabled: boolean (nullable = true)
     |    |    |-- created_at: string (nullable = true)
     |    |    |-- default_profile: boolean (nullable = true)
     |    |    |-- default_profile_image: boolean (nullable = true)
     |    |    |-- description: string (nullable = true)
     |    |    |-- favourites_count: long (nullable = true)
     |    |    |-- follow_request_sent: string (nullable = true)
     |    |    |-- followers_count: long (nullable = true)
     |    |    |-- following: string (nullable = true)
     |    |    |-- friends_count: long (nullable = true)
     |    |    |-- geo_enabled: boolean (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- is_translator: boolean (nullable = true)
     |    |    |-- lang: string (nullable = true)
     |    |    |-- listed_count: long (nullable = true)
     |    |    |-- location: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- notifications: string (nullable = true)
     |    |    |-- profile_background_color: string (nullable = true)
     |    |    |-- profile_background_image_url: string (nullable = true)
     |    |    |-- profile_background_image_url_https: string (nullable = true)
     |    |    |-- profile_background_tile: boolean (nullable = true)
     |    |    |-- profile_banner_url: string (nullable = true)
     |    |    |-- profile_image_url: string (nullable = true)
     |    |    |-- profile_image_url_https: string (nullable = true)
     |    |    |-- profile_link_color: string (nullable = true)
     |    |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |    |-- profile_text_color: string (nullable = true)
     |    |    |-- profile_use_background_image: boolean (nullable = true)
     |    |    |-- protected: boolean (nullable = true)
     |    |    |-- screen_name: string (nullable = true)
     |    |    |-- show_all_inline_media: boolean (nullable = true)
     |    |    |-- statuses_count: long (nullable = true)
     |    |    |-- time_zone: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |    |-- utc_offset: long (nullable = true)
     |    |    |-- verified: boolean (nullable = true)
     |-- source: string (nullable = true)
     |-- text: string (nullable = true)
     |-- truncated: boolean (nullable = true)
     |-- user: struct (nullable = true)
     |    |-- contributors_enabled: boolean (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- default_profile: boolean (nullable = true)
     |    |-- default_profile_image: boolean (nullable = true)
     |    |-- description: string (nullable = true)
     |    |-- favourites_count: long (nullable = true)
     |    |-- follow_request_sent: string (nullable = true)
     |    |-- followers_count: long (nullable = true)
     |    |-- following: string (nullable = true)
     |    |-- friends_count: long (nullable = true)
     |    |-- geo_enabled: boolean (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- is_translator: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- listed_count: long (nullable = true)
     |    |-- location: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- notifications: string (nullable = true)
     |    |-- profile_background_color: string (nullable = true)
     |    |-- profile_background_image_url: string (nullable = true)
     |    |-- profile_background_image_url_https: string (nullable = true)
     |    |-- profile_background_tile: boolean (nullable = true)
     |    |-- profile_banner_url: string (nullable = true)
     |    |-- profile_image_url: string (nullable = true)
     |    |-- profile_image_url_https: string (nullable = true)
     |    |-- profile_link_color: string (nullable = true)
     |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |-- profile_text_color: string (nullable = true)
     |    |-- profile_use_background_image: boolean (nullable = true)
     |    |-- protected: boolean (nullable = true)
     |    |-- screen_name: string (nullable = true)
     |    |-- show_all_inline_media: boolean (nullable = true)
     |    |-- statuses_count: long (nullable = true)
     |    |-- time_zone: string (nullable = true)
     |    |-- url: string (nullable = true)
     |    |-- utc_offset: long (nullable = true)
     |    |-- verified: boolean (nullable = true)
    



```scala
sqlC.sql("Select text, user.screen_name from twitterTable where user.screen_name='realDonaldTrump' limit 10").collect()
```




   Array([Obama asked a 7 yr old for his birth certificate. He's "in your face" because the Republicans dropped the ball. (cont) http://t.co/FufZD79U,realDonaldTrump], [Obama is taunting the Republicans on the birther issue. They should call his bluff &amp; demand the REAL facts. He (cont) http://t.co/NWmVp06e,realDonaldTrump], ["President Obama is the greatest hoax ever perpetrated on the American people"
    --Clint Eastwood,realDonaldTrump])




```scala
val trumpTweets = sqlC.sql("Select text, user.screen_name,entities from twitterTable where user.screen_name='realDonaldTrump' limit 10")
```


```scala
trumpTweets.select("text").take(10)
```




   Array([Obama asked a 7 yr old for his birth certificate. He's "in your face" because the Republicans dropped the ball. (cont) http://t.co/FufZD79U], [Obama is taunting the Republicans on the birther issue. They should call his bluff &amp; demand the REAL facts. He (cont) http://t.co/NWmVp06e], ["President Obama is the greatest hoax ever perpetrated on the American people"
    --Clint Eastwood])




```scala
val hashtags = sqlC.sql("select entities.hashtags.text from twitterTable where entities.hashtags is not null")
```
