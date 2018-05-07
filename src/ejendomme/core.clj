(ns ejendomme.core
  (:require [amazonica.aws.s3 :as s3]
            [amazonica.aws.dynamodbv2 :as ddb]
            [cheshire.core :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [com.rpl.specter :as s])
  (:import [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model GetObjectRequest ListVersionsRequest]
           [com.amazonaws.auth BasicAWSCredentials]))

(def e (decode (slurp "/Users/sla/ejendom") true))

(defn in? [coll item]
  (some #{item} coll))

(defn opret-ejendoms-versioner [n]
  (mapv #(s3/put-object :bucket-name "ejendomme"
                     :key (:vurderingsejendom_id_ice e)
                     :input-stream (java.io.ByteArrayInputStream. (.getBytes (encode (assoc (assoc-in e [:bfe 0 :sfe :jordstykke 0 :bygning 1 :etage 0 :enhed 0 :arealtilbeboelse] %) :tv (- (c/to-long (t/now)) (rand-int 100000)) :tr (c/to-long (t/now))))))) (range n)))

(defn get-versions [id & [creds]]
  (let [c (if creds (AmazonS3Client. creds) (AmazonS3Client.))
        vl (. c (listVersions (doto (ListVersionsRequest.) (.withBucketName "ejendomme") (.withPrefix id))))
        v (map #(.getVersionId %) (.getVersionSummaries vl))]
    (mapv #(. c (getObject (GetObjectRequest. "ejendomme" id %))) v)))

(def estruct {:vur-ejd {:adresse {:afstand_samletskov nil :afstand_vindmoelle nil :afstand_skov nil :afstand_vandloeb nil :afstand_soe nil :afstand_togstation nil :afstand_hoejspaending nil :afstand_jernbane nil :afstand_kystlinie nil :afstand_vej nil}
                        :bfe [{:sfe {:jordstykke [{:bygning [{:bygning_flag []
                                                              :etage [{:enhed {:lokation nil}}]}]
                                                   :tekniskanlaeg []}]}
                                :bfg [:bygning]
                               :ejerlejlighed [:enhed]}]}})

(def TM
  (s/recursive-path [] p
                    (s/cond-path
                     map? (s/continue-then-stay s/MAP-VALS p)
                     vector? [s/ALL p])))

(defn create-events [e]
  (let [vk (filter #(not (in? (keys (:vur-ejd estruct)) %)) (s/select [s/MAP-KEYS] e))
        v (assoc (select-keys e vk) :table "vur-ejd")
        ak (filter #(not (in? (get-in estruct [:vur-ejd :adresse]) %)) (s/select [:adresse s/MAP-KEYS] e))
        a (assoc (select-keys (:adresse e) ak) :table "adresse")
        bfek (filter #(not (in? (keys (get-in estruct [:vur-ejd :bfe 0])) %)) (s/select [:bfe s/ALL s/MAP-KEYS] e))
        bfe (map #(assoc (select-keys % bfek) :table "bfe") (:bfe e))
        sk (filter #(not (in? (keys (get-in estruct [:vur-ejd :bfe 0 :sfe])) %)) (s/select [:bfe s/ALL :sfe s/MAP-KEYS] e))
        s (map #(assoc (select-keys (:sfe %) sk) :table "sfe") (e :bfe))
        jk (filter #(not (in? (keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0])) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL s/MAP-KEYS] e))
        j (map #(assoc (select-keys % jk) :table "jordstykke") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL] e))
        bk (filter #(not (in? (flatten (map keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0 :bygning]))) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL s/MAP-KEYS] e))
        b (map #(assoc (select-keys % bk) :table "bygning") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL] e))
        tk (filter #(not (in? (flatten (map keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0 :tekniskanlaeg]))) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :tekniskanlaeg s/ALL s/MAP-KEYS] e))
        t (map #(assoc (select-keys % tk) :table "tekniskanlaeg") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :tekniskanlaeg s/ALL] e))
        etk (filter #(not (in? (flatten (map keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0 :bygning 1 :etage]))) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL s/MAP-KEYS] e))
        et (map #(assoc (select-keys % etk) :table "etage") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL] e))
        bfk (filter #(not (in? (flatten (map keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0 :bygning 0 :bygning_flag]))) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :bygning_flag s/ALL s/MAP-KEYS] e))
        bf (map #(assoc (select-keys % bfk) :table "bygnings_flag") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :bygning_flag s/ALL] e))
        enk (filter #(not (in? (keys (get-in estruct [:vur-ejd :bfe 0 :sfe :jordstykke 0 :bygning 1 :etage 0 :enhed])) %)) (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL :enhed s/ALL s/MAP-KEYS] e))
        en (map #(assoc (select-keys % enk) :table "enhed") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL :enhed s/ALL] e))
        l (map #(assoc % :table "lokation") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL :enhed s/ALL :lokation] e))
        p (map #(assoc % :table "plandata") (s/select [:bfe s/ALL :sfe :jordstykke s/ALL :plandata s/ALL] e))]
    [v a bfe s j b t et bf en l p]))

(defn insert-events [id]
  (let [el (flatten (create-events e))]
    (mapv #(ddb/put-item :table-name "ejendomme"
                         :return-consumed-capacity "TOTAL"
                         :return-item-collection-metrics "SIZE"
                         :item (assoc % :id id :table_tr (str (:table %) "_" (t/now)))) el)))

(defn get-events [id]
  (ddb/query
       :table-name "ejendomme"
       :select "ALL_ATTRIBUTES"
       :scan-index-forward true
       :key-conditions {:id {:attribute-value-list [id] :comparison-operator "EQ"}}))

(def traverse
  {:vurderingsejendom [:vur-ejd]
   :bfe [:vur-ejd :bfe s/ALL]
   :adresse [:vur-ejd :adresse]
   :sfe [:vur-ejd :bfe s/ALL :sfe]
   :jordstykke [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL]
   :bygning [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL]
   :tekniskanlaeg [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL :tekniskanlaeg s/ALL]
   :etage [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL]
   :enhed [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL :enhed s/ALL]
   :lokation [:vur-ejd :bfe s/ALL :sfe :jordstykke s/ALL :bygning s/ALL :etage s/ALL :enhed s/ALL :lokation]})

(defn path2 [k id is-vec?]
  (prn "P" k id is-vec?)
  (s/path (if (= k "vurderingsejendom") nil (keyword k)) (if (and (not= k "vurderingsejendom") is-vec?) s/ALL nil) #(= (% (keyword (str k "_id_ice"))) id)))

(defn path [k id is-vec?]
  (prn "P" k id is-vec?)
  (s/path (if (= k "vurderingsejendom") (vec (rest (traverse (keyword k)))))  #(= (% (keyword (str k "_id_ice"))) id)))

(defn domerge [v k d]
  (prn "M" v k d)
  (let [kk (second (filter #(not= % s/ALL) (reverse (traverse (keyword k)))))
        kk (if (nil? kk) "vurderingsejendom" (name kk))
        t (traverse (keyword kk))
        is-vec? (= (last t) s/ALL)
        id (if (= kk "vurderingsejendom") (v :vurderingsejendom_id_ice) (d (keyword (str kk "_id_ice"))))]
    (prn "K" k kk id; (s/select (path kk id is-vec?) v)
         )
    (s/transform (path kk id is-vec?) #(assoc % (keyword k) (if (= (last (traverse (keyword k))) s/ALL) [d] d)) v)))

(defn merge-event [e v events]
  (let [types (s/select (traverse e) estruct)
        path (flatten (mapv keys types))
        el (flatten (map #(filter (fn [x] (= (keyword (:table x)) %)) events) path))
        ]
   ; (prn type e path el)
    (loop [e el vv v]
      (if (empty? e)
        vv
        (recur (rest e) (domerge vv (:table (first e)) (first e)))))
   ; v
    ))

(defn merge-events [events vurejd]
  (loop [e (keys traverse) v vurejd]
    (if (empty? e)
      v
      (recur (rest e) (merge-event (first e) v events)))))

(defn collect-ejendom [id & [tv tr]]
  (let [e (:items (get-events id))
        v (first (filter #(= "vur-ejd" (:table %)) e))]
    (merge-events e  v
                  )
    ))
