(ns lein-test.db-helpers
  (:require
   [lein-test.constants :refer [ENTITIES]]
   [honey.sql :as sql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [next.jdbc.connection :as connection])
  (:import (com.zaxxer.hikari HikariDataSource)))

(def ds (connection/->pool HikariDataSource
                           {:dbtype "postgres" :dbname "geo-global" :username "postgres" :password "fart" :maximumPoolSize 10
                            :dataSourceProperties {:socketTimeout 30}}))

(defn try-execute [query]
  (try
    (jdbc/execute! ds query)
    (catch Exception e
      (println e "Failed to execute query!" query))))





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
   (-> (h/create-table (keyword defined-in entity-id) :if-not-exists)
       (h/with-columns
         [:id :text [:not nil]]
         [:entity_id :text [:not nil] [:references :public/entities :id]])
       (sql/format  {:pretty true}))))

(defn create-type-tables
  "Creates tables for all types in the triple store"
  []
  (doseq [type (all-types)]
    (create-type-table (:triples/defined_in type) (:triples/entity_id type))))

(defn- all-spaces
  "Returns a seq of all distinct spaces"
  []
  (try-execute (->
                     (h/select-distinct-on [:address] :address)
                     (h/from :spaces)
                     (sql/format {:pretty true}))))

(defn- make-schema
  "Creates a schema"
  [space]
  (try-execute (sql/format [:raw (str "CREATE SCHEMA IF NOT EXISTS \"" space "\"")] {:pretty true})))

(defn make-space-schemas
  []
  (doseq [space (all-spaces)]
    (make-schema (:spaces/address space))))

;; Helpers to reset the db for testing
(def nuke-actions (-> (h/delete-from :actions)
                      (h/where [:= :value-type nil])
                      (sql/format {:pretty true})))

(def nuke-entities (-> (h/delete-from :entities)
                       (h/where [:= :value-type nil])
                       (sql/format {:pretty true})))

(def nuke-entity-types (-> (h/delete-from :entity_types)
                           (h/where [:not= :id ""])
                           (sql/format {:pretty true})))

(def nuke-entity-attributes (-> (h/delete-from :entity_attributes)
                                (h/where [:not= :id ""])
                                (sql/format {:pretty true})))

(def nuke-spaces (-> (h/delete-from :spaces)
                     (h/where [:not= :id ""])
                     (sql/format {:pretty true})))

(def nuke-triples (-> (h/delete-from :triples)
                      (h/where [:= :is_protected false])
                      (sql/format {:pretty true})))
(def get-schemas  (-> (h/select :schema-name)
                      (h/from :information_schema.schemata)
                      (h/where [:like :schema_name "0x%"])
                      (sql/format {:pretty true})))

(defn nuke-schemas []
  (doseq [schema (try-execute get-schemas)]
    (try-execute (sql/format [:raw (str "DROP SCHEMA IF EXISTS \"" (:schemata/schema_name schema) "\" CASCADE")] {:pretty true}))))

(defn nuke-db []
  (jdbc/execute! ds nuke-actions)
  (jdbc/execute! ds nuke-entities)
  (jdbc/execute! ds nuke-entity-types)
  (jdbc/execute! ds nuke-entity-attributes)
  (jdbc/execute! ds nuke-triples)
  (jdbc/execute! ds nuke-spaces)
  (nuke-schemas))
