(ns metabase.driver.databricks-sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honey.sql :as sql]            
            [metabase.util.honey-sql-2 :as h2x]
            [medley.core :as m]
            [metabase.driver :as driver] 
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util :as sql.u]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.util :as qp.util]
            [java-time :as t]
            [metabase.util :as u]
            [metabase.util.date-2 :as u.date])            
  (:import [java.sql Connection ResultSet Types]
           [java.time LocalDate OffsetDateTime ZonedDateTime]))

(driver/register! :databricks-sql, :parent :sql-jdbc)

(defmethod sql-jdbc.conn/connection-details->spec :databricks-sql
  [_ {:keys [host http-path password db catalog]}]
  {:classname        "com.databricks.client.jdbc.Driver"
   :subprotocol      "databricks"
   :subname          (str "//" host ":443/" db)
   :transportMode    "http"
   :ssl              1
   :AuthMech         3
   :httpPath         http-path
   :uid              "token"
   :pwd              password
   :connCatalog      catalog
   })

(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :databricks-sql
  [driver database]
  ;; The Hive JDBC driver doesn't support `Connection.isValid()`, so we need to supply a test query for c3p0 to use to
  ;; validate connections upon checkout.
  (merge
   ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver database)
   {"preferredTestQuery" "SELECT 1"}))

(defmethod sql-jdbc.sync/database-type->base-type :databricks-sql
  [_ database-type]
  (condp re-matches (name database-type)
    #"boolean"          :type/Boolean
    #"tinyint"          :type/Integer
    #"smallint"         :type/Integer
    #"int"              :type/Integer
    #"bigint"           :type/BigInteger
    #"float"            :type/Float
    #"double"           :type/Float
    #"double precision" :type/Double
    #"decimal.*"        :type/Decimal
    #"char.*"           :type/Text
    #"varchar.*"        :type/Text
    #"string.*"         :type/Text
    #"binary*"          :type/*
    #"date"             :type/Date
    #"time"             :type/Time
    #"timestamp"        :type/DateTime
    #"interval"         :type/*
    #"array.*"          :type/Array
    #"map"              :type/Dictionary
    #".*"               :type/*))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-database :databricks-sql
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [database tablename], table-namespace :namespace} (jdbc/query {:connection conn} ["show tables"])]
        {:name   tablename
         :schema (or (not-empty database)
                     (not-empty table-namespace))})))})

;; Hive describe table result has commented rows to distinguish partitions
(defn- valid-describe-table-row? [{:keys [col_name data_type]}]
  (every? (every-pred (complement str/blank?)
                      (complement #(str/starts-with? % "#")))
          [col_name data_type]))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"-" "_")))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-table :databricks-sql
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(format
                                                    "describe %s"
                                                    (sql.u/quote-name driver :table
                                                                      (dash-to-underscore schema)
                                                                      (dash-to-underscore table-name)))])]
       (set
        (for [[idx {col-name :col_name, data-type :data_type, :as result}] (m/indexed results)
              :while (valid-describe-table-row? result)]
          {:name              col-name
           :database-type     data-type
           :base-type         (sql-jdbc.sync/database-type->base-type :databricks-sql (keyword data-type))
           :database-position idx}))))})

(def ^:dynamic *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

;; bound variables are not supported in Spark SQL (maybe not Hive either, haven't checked)
(defmethod driver/execute-reducible-query :databricks-sql
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                               :remark (qp.util/query->remark :databricks-sql outer-query)
                               :query  (if (seq params)
                                         (binding [*param-splice-style* :paranoid]
                                           (unprepare/unprepare driver (cons sql params)))
                                         sql)
                               :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query       (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))

;; 1.  SparkSQL doesn't support `.supportsTransactionIsolationLevel`
;; 2.  SparkSQL doesn't support session timezones (at least our driver doesn't support it)
;; 3.  SparkSQL doesn't support making connections read-only
;; 4.  SparkSQL doesn't support setting the default result set holdability
(defmethod sql-jdbc.execute/connection-with-timezone :databricks-sql
  [driver database _timezone-id]
  (let [conn (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))]
    (try
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_UNCOMMITTED)
      conn
      (catch Throwable e
        (.close conn)
        (throw e)))))

;; 1.  SparkSQL doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :databricks-sql
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; the current HiveConnection doesn't support .createStatement
(defmethod sql-jdbc.execute/statement-supported? :databricks-sql [_] false)

(doseq [feature [:basic-aggregations
                 :binning
                 :expression-aggregations
                 :expressions
                 :native-parameters
                 :nested-queries
                 :standard-deviation-aggregations]]
  (defmethod driver/supports? [:databricks-sql feature] [_ _] true))

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/supports?) [:databricks-sql :foreign-keys])
  (defmethod driver/supports? [:databricks-sql :foreign-keys] [_ _] true))

(defmethod sql.qp/quote-style :databricks-sql [_] :mysql)

(defn- extract
  [unit expr]
  (-> [:date_part unit (h2x/->timestamp expr)]
      (h2x/with-database-type-info "integer")))

(defn- date-trunc
  [unit expr]
  (let [acceptable-types (case unit
                           (:millisecond :second :minute :hour) #{"time" "timestamp"}
                           (:day :week :month :quarter :year)   #{"date" "timestamp"})
        expr             (h2x/cast-unless-type-in "timestamp" acceptable-types expr)]
    (-> [:date_trunc (h2x/literal unit) expr]
        (h2x/with-database-type-info (h2x/database-type expr)))))

(defmethod sql.qp/date [:databricks-sql :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:databricks-sql :minute]          [_ _ expr] (date-trunc :minute expr))
(defmethod sql.qp/date [:databricks-sql :minute-of-hour]  [_ _ expr] (extract :minute expr))
(defmethod sql.qp/date [:databricks-sql :hour]            [_ _ expr] (date-trunc :hour expr))
(defmethod sql.qp/date [:databricks-sql :hour-of-day]     [_ _ expr] (extract :hour expr))
(defmethod sql.qp/date [:databricks-sql :day]             [_ _ expr] (date-trunc :day expr))
(defmethod sql.qp/date [:databricks-sql :day-of-month]    [_ _ expr] (extract :day expr))
(defmethod sql.qp/date [:databricks-sql :day-of-year]     [_ _ expr] (extract :dayofyear expr))
(defmethod sql.qp/date [:databricks-sql :month]           [_ _ expr] (date-trunc :month expr))
(defmethod sql.qp/date [:databricks-sql :month-of-year]   [_ _ expr] (extract :month expr))
(defmethod sql.qp/date [:databricks-sql :quarter]         [_ _ expr] (date-trunc :quarter expr))
(defmethod sql.qp/date [:databricks-sql :quarter-of-year] [_ _ expr] (extract :quarter expr))
(defmethod sql.qp/date [:databricks-sql :year]            [_ _ expr] (date-trunc :year expr))
(defmethod driver/db-start-of-week :databricks-sql
  [_]
  :sunday)

(def ^:private date-extract-units
  "See https://spark.apache.org/docs/3.3.0/api/sql/#extract"
  #{:year :y :years :yr :yrs
    :yearofweek
    :quarter :qtr
    :month :mon :mons :months
    :week :w :weeks
    :day :d :days
    :dayofweek :dow
    :dayofweek_iso :dow_iso
    :doy
    :hour :h :hours :hr :hrs
    :minute :m :min :mins :minutes
    :second :s :sec :seconds :secs})

(defn- format-date-extract
  [_fn [unit expr]]
  {:pre [(contains? date-extract-units unit)]}
  (let [[expr-sql & expr-args] (sql/format-expr expr {:nested true})]
    (into [(format "extract(%s FROM %s)" (name unit) expr-sql)]
          expr-args)))

(sql/register-fn! ::date-extract #'format-date-extract)

(defn- format-interval
  "Interval actually supports more than just plain numbers, but that's all we currently need. See
  https://spark.apache.org/docs/latest/sql-ref-literals.html#interval-literal"
  [_fn [amount unit]]
  {:pre [(number? amount)
         ;; other units are supported too but we're not currently supporting them.
         (#{:year :month :week :day :hour :minute :second :millisecond} unit)]}
  [(format "(interval '%d' %s)" (long amount) (name unit))])

(sql/register-fn! ::interval #'format-interval)

(defmethod sql.qp/date [:databricks-sql :day-of-week]
  [driver _unit expr]
  (sql.qp/adjust-day-of-week driver (-> [::date-extract :dow (h2x/->timestamp expr)]
                                        (h2x/with-database-type-info "integer"))))

(defmethod sql.qp/date [:databricks-sql :week]
  [driver _unit expr]
  (let [week-extract-fn (fn [expr]
                          (-> [:date_sub
                               (h2x/+ (h2x/->timestamp expr)
                                      [::interval 1 :day])
                               [::date-extract :dow (h2x/->timestamp expr)]]
                              (h2x/with-database-type-info "timestamp")))]
    (sql.qp/adjust-start-of-week driver week-extract-fn expr)))

(defmethod sql.qp/date [:databricks-sql :week-of-year-iso]
  [_driver _unit expr]
  [:weekofyear (h2x/->timestamp expr)])

(defmethod sql.qp/date [:databricks-sql :quarter]
  [_driver _unit expr]
  [:add_months
   [:trunc (h2x/->timestamp expr) (h2x/literal :year)]
   (h2x/* (h2x/- [:quarter (h2x/->timestamp expr)]
                 1)
          3)])

(defmethod sql.qp/->honeysql [:databricks-sql :replace]
  [driver [_ arg pattern replacement]]
  [:regexp_replace
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver pattern)
   (sql.qp/->honeysql driver replacement)])

(defmethod sql.qp/->honeysql [:databricks-sql :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) 0])

(defmethod sql.qp/->honeysql [:databricks-sql :median]
  [driver [_ arg]]
  [:percentile (sql.qp/->honeysql driver arg) 0.5])

(defmethod sql.qp/->honeysql [:databricks-sql :percentile]
  [driver [_ arg p]]
  [:percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

(defmethod sql.qp/add-interval-honeysql-form :databricks-sql
  [_ hsql-form amount unit]
  [:dateadd
   [:raw (name unit)]
   [:raw (int amount)]
   (h2x/->timestamp hsql-form)])

(defmethod sql.qp/datetime-diff [:databricks-sql :year]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :month x y) 12])

(defmethod sql.qp/datetime-diff [:databricks-sql :quarter]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :month x y) 3])

(defmethod sql.qp/datetime-diff [:databricks-sql :month]
  [_driver _unit x y]
  (h2x/->integer [:months_between y x]))

(defmethod sql.qp/datetime-diff [:databricks-sql :week]
  [_driver _unit x y]
  [:div [:datediff y x] 7])

(defmethod sql.qp/datetime-diff [:databricks-sql :day]
  [_driver _unit x y]
  [:datediff y x])

(defmethod sql.qp/datetime-diff [:databricks-sql :hour]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :second x y) 3600])

(defmethod sql.qp/datetime-diff [:databricks-sql :minute]
  [driver _unit x y]
  [:div (sql.qp/datetime-diff driver :second x y) 60])

(defmethod sql.qp/datetime-diff [:databricks-sql :second]
  [_driver _unit x y]
  [:- [:unix_timestamp y] [:unix_timestamp x]])

(def ^:dynamic *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

;; (defmethod unprepare/unprepare-value [:databricks-sql String]
;;   [_ ^String s]
;;   ;; Because Spark SQL doesn't support parameterized queries (e.g. `?`) convert the entire String to hex and decode.
;;   ;; e.g. encode `abc` as `decode(unhex('616263'), 'utf-8')` to prevent SQL injection
;;   (case *param-splice-style*
;;     :friendly (str \' (sql.u/escape-sql s :backslashes) \')
;;     :paranoid (format "decode(unhex('%s'), 'utf-8')" (codecs/bytes->hex (.getBytes s "UTF-8")))))

;; Hive/Spark SQL doesn't seem to like DATEs so convert it to a DATETIME first
(defmethod unprepare/unprepare-value [:databricks-sql LocalDate]
  [driver t]
  (unprepare/unprepare-value driver (t/local-date-time t (t/local-time 0))))

(defmethod unprepare/unprepare-value [:databricks-sql OffsetDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:databricks-sql ZonedDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-id t)))

;; Hive/Spark SQL doesn't seem to like DATEs so convert it to a DATETIME first
(defmethod sql-jdbc.execute/set-parameter [:databricks-sql LocalDate]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/local-date-time t (t/local-time 0))))

;; TIMEZONE FIXME — not sure what timezone the results actually come back as
(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/TIME]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/offset-time (t/local-time t) (t/zone-offset 0)))))

(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/DATE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getDate rs i)]
      (t/zoned-date-time (t/local-date t) (t/local-time 0) (t/zone-id "UTC")))))

(defmethod sql-jdbc.execute/read-column-thunk [:databricks-sql Types/TIMESTAMP]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))

(defmethod sql.qp/current-datetime-honeysql-form :databricks-sql
  [_]
  :%getdate)