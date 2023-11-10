(ns geo-sink.db-helpers
  (:require [clojure.string]
            [dotenv :refer [env]]
            [geo-sink.constants :refer [ATTRIBUTES ENTITIES
                                        geo-genesis-start-block ROOT-SPACE-ADDRESS]]
            [geo-sink.tables :refer [generate-triple-id]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (com.zaxxer.hikari HikariDataSource)))


(def ds (connection/->pool HikariDataSource
                           {:dbtype "postgres" :dbname (env "PGDATABASE") :host (env "PGHOST") :port (env "PGPORT") :username (env "PGUSER") :password (env "PGPASSWORD") :maximumPoolSize 10
                            :dataSourceProperties {:socketTimeout 30}}))

(defn try-execute [query]
  (try
    (jdbc/execute! ds query)
    (catch Exception e
      (println e "Failed to execute query!" query))))

(defn update-db-cursor
  [cursor-string block-number]
  (-> (h/insert-into :public/cursors)
      (h/values [{:id 0 :cursor cursor-string :block-number block-number}])
      (h/on-conflict :id (h/do-update-set :cursor :block-number))
      (sql/format)
      try-execute))

(defn get-cursor
  []
  (-> (h/select :cursor :block-number)
      (h/from :public/cursors)
      (h/where [:= :id 0])
      (sql/format)
      try-execute
      first))







(defn- all-spaces
  "Returns a seq of all distinct spaces"
  []
  (try-execute (->
                (h/select-distinct-on [:address] :address)
                (h/from :spaces)
                (sql/format {:pretty true}))))



(defn get-all-attribute-entities
  "Gets all rows from :public/entities where is_attribute is true"
  []
  (try-execute (-> (h/select [:*])
                   (h/from :public/entities)
                   (h/where [:and [:= :is_attribute true]])
                   (sql/format {:pretty true}))))

(defn get-all-type-entities
  "Gets all rows from :public/entities where is_attribute is true"
  []
  (try-execute (-> (h/select [:*])
                   (h/from :public/entities)
                   (h/where [:and [:= :is_type true]])
                   (sql/format {:pretty true}))))



(defn- make-schema
  "Creates a schema"
  [space]
  (try-execute (sql/format [:raw (str "CREATE SCHEMA IF NOT EXISTS \"" (.toLowerCase space) "\"")] {:pretty true})))

;; Future: Implement space-specific schemas
(defn make-space-schemas
  []
  (doseq [space (all-spaces)]
    (make-schema (:spaces/address space))))

(def get-schemas  (-> (h/select :schema-name)
                      (h/from :information_schema.schemata)
                      (h/where [:like :schema_name "0x%"])
                      (sql/format {:pretty true})))

(defn nuke-db []
  (try-execute (sql/format [:raw (slurp "./src/geo_sink/sql/nuke.sql")])))

(defn bootstrap-db []
  (try-execute (sql/format [:raw (slurp "./src/geo_sink/migrations/001_bootstrap.sql")])))

(defn bootstrap-entities
  []
  ;; creates the entities
  (try-execute  (-> (h/insert-into :public/cursors)
                        (h/values [{:id 0 :cursor "" :block-number geo-genesis-start-block}])
                        (sql/format)))

  (try-execute (-> (h/insert-into :public/entities)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)]
                                                               {:id (:id entity)
                                                                :name (:name entity)
                                                                :is_type true
                                                                :defined_in ROOT-SPACE-ADDRESS})) ENTITIES)))
                        sql/format))

  ;; creates the attributes
  (try-execute (-> (h/insert-into :public/entities)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)]
                                                               {:id (:id entity)
                                                                :name (:name entity)
                                                                :is_type true
                                                                :defined_in ROOT-SPACE-ADDRESS
                                                                :attribute_value_type_id (:id ((:value-type entity) ENTITIES))})) ATTRIBUTES)))
                        sql/format))

  ;; creates the triples giving the entities a type of type
  (try-execute (-> (h/insert-into :public/triples)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)
                                                                   entity-id (:id entity)
                                                                   attribute-id (:id (:type ATTRIBUTES))
                                                                   value-id (:id (:schema-type ENTITIES))]
                                                               {:id (generate-triple-id ROOT-SPACE-ADDRESS entity-id attribute-id value-id)
                                                                :entity_id entity-id
                                                                :attribute_id attribute-id
                                                                :value_id value-id
                                                                :value_type "entity"
                                                                :entity_value value-id
                                                                :defined_in ROOT-SPACE-ADDRESS
                                                                :is_protected true
                                                                :deleted false})) ENTITIES)))
                        sql/format))

  ;; creates the triples giving the entities a type of attribute
  (try-execute (-> (h/insert-into :public/triples)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)
                                                                   entity-id (:id entity)
                                                                   attribute-id (:id (:type ATTRIBUTES))
                                                                   value-id (:id (:attribute ENTITIES))]
                                                               {:id (generate-triple-id ROOT-SPACE-ADDRESS entity-id attribute-id value-id)
                                                                :entity_id entity-id
                                                                :attribute_id attribute-id
                                                                :value_id value-id
                                                                :value_type "entity"
                                                                :entity_value value-id
                                                                :defined_in ROOT-SPACE-ADDRESS
                                                                :is_protected true
                                                                :deleted false})) ATTRIBUTES)))
                        sql/format))


  (-> (h/insert-into :public/spaces)
      (h/values [{:id "root_space"
                  :address ROOT-SPACE-ADDRESS
                  :is-root-space true}])
      (sql/format)
      try-execute))

(defn reset-geo-db
  []
  (println "Nuking db...")
  (nuke-db)
  (println "Bootstrapping db schema...")
  (bootstrap-db)
  (println "Bootstrapping initial entities...")
  (bootstrap-entities))

