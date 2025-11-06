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
  ([mng sql]
   (jnor/typed [JdbcManager mng] [String sql])
   (.execute mng sql))
  ([mng sql values]
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
  [result-set cols]
  (let [key-name-entries (map (fn [k] [k (name k)]) cols)]
    (->> (repeat result-set)
         (take-while #(.next %))
         (map (fn [rs] (->> key-name-entries
                            (mapcat (fn [[k n]] [k (.getObject rs n)]))
                            (apply hash-map))))
         vec)))

(defn exec-query!
  "Returns the result of executing the `sql` string with the JDBC manager
  `mng`.
  `cols` is a set of keywords that match the names of the desired columns to
  retrieve.
  A `values` vector is used when `sql` has ? placeholders."
  ([mng sql cols]
   (jnor/typed [JdbcManager mng] [String sql] [IPersistentSet cols])
   (result-set->vec (.executeQuery mng sql) cols))
  ([mng sql values cols]
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
