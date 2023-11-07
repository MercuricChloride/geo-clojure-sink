(ns lein-test.db-helpers
  (:require [clojure.string]
            [dotenv :refer [env]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [lein-test.constants :refer [ATTRIBUTES ENTITIES
                                         ROOT-SPACE-ADDRESS]]
            [lein-test.tables :refer [generate-triple-id]]
            [next.jdbc :as jdbc]
            [next.jdbc.connection :as connection])
  (:import (com.zaxxer.hikari HikariDataSource)))

(def ds (connection/->pool HikariDataSource
                           {:dbtype "postgres" :dbname (env "GEO_DB_NAME") :username (env "GEO_DB_USERNAME") :password (env "GEO_DB_PASSWORD") :maximumPoolSize 10
                            :dataSourceProperties {:socketTimeout 30}}))


(defn try-execute [query]
  (try
    (jdbc/execute! ds query)
    (catch Exception e
      (println e "Failed to execute query!" query))))

(defn update-cursor
  [cursor-string block-number]
  (-> (h/insert-into :cursors)
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
     

(defn- all-types
  "Returns a seq of all types in the triple store"
  []
  (try-execute (->
                (h/select-distinct-on [:entity_id] :entity_id :defined_in)
                (h/from :triples)
                (h/where [:= :attribute_id "type"])
                (h/where [:= :value_id (:id (:schema-type ENTITIES))])
                (sql/format {:pretty true}))))

(defn- create-type-table
  "Creates a table for a type in the schema it was defined in."
  [defined-in entity-id]
  (try-execute
   (-> (h/create-table (str defined-in "." entity-id) :if-not-exists)
       (h/with-columns
         [:id :text [:not nil]]
         [:entity_id :text [:not nil] [:references :public/entities :id]])
       (sql/format  {:pretty true}))))

(defn- upsert-is-type
  [entity-id]
  (try-execute (-> (h/update :entities)
                   (h/set {:is_type true})
                   (h/where [:= :id entity-id])
                   (sql/format {:pretty true}))))

(defn create-type-tables
  "Creates tables for all types in the triple store"
  []
  (doseq [type (all-types)]
    (upsert-is-type (:triples/entity_id type))
    (create-type-table (:triples/defined_in type) (:triples/entity_id type))))

(defn- all-spaces
  "Returns a seq of all distinct spaces"
  []
  (try-execute (->
                (h/select-distinct-on [:address] :address)
                (h/from :spaces)
                (sql/format {:pretty true}))))



(defn get-all-schema-type-entities
  "Gets all rows from :public/entities where is_type is true"
  []
  (try-execute (-> (h/select [:*])
                   (h/from :public/entities)
                   (h/where [:= :is_type true])
                   (sql/format {:pretty true}))))

(defn get-all-attribute-entities  
  "Gets all rows from :public/entities where is_attribute is true"
  []
  (try-execute (-> (h/select [:*])
                   (h/from :public/entities)
                   (h/where [:and [:= :is_attribute true]])
                   (sql/format {:pretty true}))))

(defn get-entity-name
  "Returns the name of the entity for a given entity-id"
  [entity-id]
  (try-execute (-> (h/select [:name])
                   (h/from :entities)
                   (h/where [:= :id entity-id])
                   (sql/format))))
;(time (all-names))

(defn upsert-names
  "Upserts all names for entities"
  [names]
  (try-execute (-> (h/insert-into :entities)
                   (h/values (into [] (map (fn [name] {:id (:triples/entity_id name)
                                                       :name (:triples/string_value name)}) names)))
                   (h/on-conflict :id (h/do-update-set :name))
                   (sql/format {:pretty true}))))

;(def test-names (take 10000 (all-names)))

;; (time (doseq [name test-names]
;;     (try-execute (-> (h/update :entities)
;;                      (h/set {:name (:triples/string_value name)})
;;                      (h/where [:= :id (:triples/entity_id name)])
;;                      (sql/format {:pretty true})))))

;; (time (try-execute (-> (h/update :entities)
;;                      (h/set {:name (:triples/string_value test-name)})
;;                      (h/where [:= :id (:triples/entity_id test-name)])
;;                      (sql/format {:pretty true}))))

(defn- make-schema
  "Creates a schema"
  [space]
  (try-execute (sql/format [:raw (str "CREATE SCHEMA IF NOT EXISTS \"" (.toLowerCase space) "\"")] {:pretty true})))

(defn make-space-schemas
  []
  (doseq [space (all-spaces)]
    (make-schema (:spaces/address space))))

(def get-schemas  (-> (h/select :schema-name)
                      (h/from :information_schema.schemata)
                      (h/where [:like :schema_name "0x%"])
                      (sql/format {:pretty true})))

(defn nuke-schemas []
  (doseq [schema (try-execute get-schemas)]
    (try-execute (sql/format [:raw (str "DROP SCHEMA IF EXISTS \"" (:schemata/schema_name schema) "\" CASCADE")] {:pretty true}))))


(defn nuke-db []
  (jdbc/execute! ds (sql/format [:raw (slurp "src/lein_test/sql/nuke.sql")])))

;; (nuke-db)

(defn bootstrap-db []
  (jdbc/execute! ds (sql/format [:raw (slurp "src/lein_test/migrations/001_bootstrap.sql")])))

(defn bootstrap-entities
  []
  ;; creates the entities
  (jdbc/execute! ds (-> (h/insert-into :public/entities)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)]
                                                               {:id (:id entity)
                                                                :name (:name entity)
                                                                :is_type true
                                                                :defined_in ROOT-SPACE-ADDRESS})) ENTITIES)))
                        sql/format))

  ;; creates the attributes
  (jdbc/execute! ds (-> (h/insert-into :public/entities)
                        (h/values (into [] (map (fn [entity] (let [entity (second entity)]
                                                               {:id (:id entity)
                                                                :name (:name entity)
                                                                :is_type true
                                                                :defined_in ROOT-SPACE-ADDRESS
                                                                :attribute_value_type_id (:id ((:value-type entity) ENTITIES))})) ATTRIBUTES)))
                        sql/format))

  ;; creates the triples giving the entities a type of type
  (jdbc/execute! ds (-> (h/insert-into :public/triples)
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
  (jdbc/execute! ds (-> (h/insert-into :public/triples)
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
                        sql/format)))

;(bootstrap-db)
;(bootstrap-entities)

;(nuke-db)
