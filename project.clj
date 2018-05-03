(defproject ejendomme "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.6.3"]
                 [clj-time "0.14.2"]
                 [com.rpl/specter "1.1.1"]
                 [danlentz/clj-uuid "0.1.7"]
                 [amazonica "0.3.121" :exclusions [com.amazonaws/aws-java-sdk
                                                   com.amazonaws/amazon-kinesis-client]]
                 [com.amazonaws/aws-java-sdk-core "1.11.304"]
                 [com.amazonaws/aws-java-sdk-s3 "1.11.304"]
                 [com.amazonaws/aws-java-sdk-dynamodb "1.11.304"]])
