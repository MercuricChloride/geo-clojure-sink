(ns lein-test.core
  (:gen-class)
  (:require
   [cheshire.core :as ch]
   [clojure.spec.alpha :as s]
   [clojure.string :as cstr]
   [clojure.core.reducers :as r]
   [clojure.java.io :as io]
   [lein-test.spec.action :as action]
   [honey.sql :as sql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as connection]
   [lein-test.spec.action-type :as at]
   [lein-test.constants :refer [ROOT-SPACE-ADDRESS]])
  (:import (com.zaxxer.hikari HikariDataSource)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(def ds (connection/->pool com.zaxxer.hikari.HikariDataSource
                   {:dbtype "postgres" :dbname "geo-global" :username "postgres" :password "fart" :maximumPoolSize 10
                    :dataSourceProperties {:socketTimeout 30}}))
;; (def pg-db {:dbtype "postgresql"
;;             :dbname "geo-global"
;;             :host "localhost"
;;             :user "postgres"
;;             :password "fart"})

;(def ds (jdbc/get-datasource datasource-options))


(defn action->struct [action]
  (let [type (:type action) entityId (:entityId action) attributeId (:attributeId action)]
    {
   :id (str type "-" entityId "-" attributeId)
   :action_type type
   :entity entityId
   :attribute attributeId
   }))

(defn action->entity [action]
  (let [entityId (:entityId action)]
    {:id entityId}))

(defn- validate-actions [actions]
  (filter #(action/valid-action? %) actions))

(defn- json->actions
 ([path]
  (let [json (slurp path)]
    (validate-actions ((ch/parse-string json true) :actions))))
 ([prefix path]
  (let [json (slurp (str prefix path))]
    (validate-actions ((ch/parse-string json true) :actions)))))

(def actions (validate-actions (json->actions "geo-data.json")))

(defn retry-future
  "Tries to execute a future n times, backing for 5 seconds longer each time it returns false"
  [f n]
  (loop
   [tries 0]
    (when (> n tries)
      (Thread/sleep (* tries 5000))
      (let [val @(f)]
        (if val
          val
            (recur (inc tries)))))))

(defmacro try3 [future]
  `(retry-future (fn [] ~future) 3))

(defn handle-execute [input]
  (let [dependencies (:dependencies input) handler (:handler input)]
    (future
      (try3 dependencies)
      @handler)))

;(time (doall (map #(deref % 5000 "timed out") (map #(handle-execute (at/execute %)) (filter #(at/sink-action %) long-actions)))))


;(println (time (jdbc/execute! ds query-str)))
;(jdbc/execute! ds nuke-actions)

(def nuke-actions (-> (h/delete-from :actions)
                      (h/where [:= :value-type nil])
                      (sql/format {:pretty true})))

(def nuke-entities (-> (h/delete-from :entities)
                      (h/where [:= :value-type nil])
                      (sql/format {:pretty true})))
(jdbc/execute! ds nuke-actions)
(jdbc/execute! ds nuke-entities)

(defn query-str [actions]
(-> (h/insert-into :actions)
    (h/values (into [] (map action->struct actions)))
    (h/on-conflict :id (h/do-nothing))
    (sql/format {:pretty true})))

(defn entity-query [actions]
(-> (h/insert-into :entities)
    (h/values (into [] (map action->entity actions)))
    (h/on-conflict :id (h/do-nothing))
    (sql/format {:pretty true})))

(defn entity-exists [entity-id]
  (-> (h/select :*)
      (h/from :entities)
      (h/where [:= :id entity-id])
      (sql/format {:pretty true})))

(defn table-key [space address]
  (keyword space address))

(table-key ROOT-SPACE-ADDRESS "foo")

(sql/format {:alter-table :public/entities :add-column [:id :text [:not nil] :if-not-exists]})

(defn entity->table [entity]
  (keyword (:entities/defined-in entity) (:entities/id entity)))

(def files (->> (io/file "./action_cache/")
               file-seq
               rest
               (map #(cstr/replace % #"./action_cache/" ""))
               (sort-by #(get (cstr/split % #"_" ) 0))
               (map #(json->actions "./action_cache/" %))))


(defn try-execute [query]
  (try
    (jdbc/execute! ds query)
    (catch Exception e
      (println e "failed to insert entity" query))))

(time (map #(do (try-execute (entity-query %))(try-execute (query-str %))) files))
