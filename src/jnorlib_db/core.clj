(ns jnorlib-db.core
  "Higher-level JDBC API."
  (:require [jnorlib.core :as jnor])
  (:import [clojure.lang IPersistentSet IPersistentVector]
           [jnorlib_db JdbcManager]))

(defn manager
  "Creates an object that may auto-close the JDBC resources produced by the
  provided `javax.sql.DataSource` as `ds`."
  [ds]
  (JdbcManager. ds))

(defn exec!
  "Executes the `sql` string with the JDBC manager `mng`.
  A `values` vector is used when `sql` has ? placeholders."
  ([mng ^String sql]
   (jnor/typed [JdbcManager mng] [String sql])
   (.execute mng sql))
  ([mng ^String sql values]
   (jnor/typed [JdbcManager mng] [String sql] [IPersistentVector values])
   (let [ps (.prepareStatement mng sql)
         object-count (count values)]
     (loop [i 0
            ordinal 1]
       (if (> ordinal object-count)
         (do (.execute mng ps) nil)
         (do
           (.setObject ps ordinal (values i))
           (recur (inc i) (inc ordinal))))))))

(defn- result-set->vec
  [rs cols]
  (->> (repeat rs)
       (take-while #(.next %))
       (map #(->> cols
                  (reduce
                   (fn [m col] (assoc! m col (.getObject % (name col))))
                   (transient {}))
                  persistent!))
       vec))

(defn exec-query!
  "Returns the result of executing the `sql` string with the JDBC manager
  `mng`.
  `cols` is a set of keywords that match the names of the desired columns to
  retrieve.
  A `values` vector is used when `sql` has ? placeholders."
  ([mng ^String sql cols]
   (jnor/typed [JdbcManager mng] [String sql] [IPersistentSet cols])
   (result-set->vec (.executeQuery mng sql) cols))
  ([mng ^String sql values cols]
   (jnor/typed
    [JdbcManager mng]
    [String sql]
    [IPersistentVector values]
    [IPersistentSet cols])
   (let [ps (.prepareStatement mng sql)
         object-count (count values)]
     (loop [i 0
            ordinal 1]
       (if (> ordinal object-count)
         (result-set->vec (.executeQuery mng ps) cols)
         (do
           (.setObject ps ordinal (values i))
           (recur (inc i) (inc ordinal))))))))
