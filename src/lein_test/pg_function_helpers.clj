(ns lein-test.pg-function-helpers
  (:require [clojure.string :as s]
            [honey.sql :as sql]
            [lein-test.constants :refer [ATTRIBUTES]]
            [lein-test.db-helpers :refer [get-all-attribute-entities
                                          try-execute]]))

(def type-id (str (:id (:type ATTRIBUTES))))
(def attribute-id (str (:id (:attribute ATTRIBUTES))))

(defn fn-entities-types
  "Used to get all of the types of an entity.
   
   query Example {
      entities {
        id
        name
        types {
          id
          name
        } 
      }
  }"
  []
  (str "CREATE OR REPLACE FUNCTION entities_types(e_row entities)
    RETURNS SETOF entities AS $$
    BEGIN
        RETURN QUERY
        SELECT e.*
        FROM entities e
        WHERE e.id IN (
            SELECT t.value_id
            FROM triples t
            WHERE t.entity_id = e_row.id AND t.attribute_id = '" type-id "'
        );
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;"))


(defn fn-entities-type-count
  "Used to get the count of all types of an entity.
   
   query Example {
      entities {
        id
        name
        typeCount
      }
  }"
  []
  (str "CREATE OR REPLACE FUNCTION entities_type_count(e_row entities)
    RETURNS integer AS $$
    DECLARE
        type_count integer;
    BEGIN
        SELECT count(*)
        INTO type_count
        FROM entities_types(e_row);
        RETURN type_count;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;"))

(defn fn-entities-attributes
  "Used to get all of the attributes of a schema type.
   
   query Example {
      entities {
        id
        name
        attributes {
          id
          name
          valueType {
            id
            name
          }
        } 
      }
  }"
  []
  (str "CREATE OR REPLACE FUNCTION entities_attributes(e_row entities)
    RETURNS SETOF entities AS $$
    BEGIN
      RETURN QUERY
      SELECT e.*
            FROM entities e
            WHERE e.id IN (
                SELECT t.value_id
                FROM triples t
                WHERE t.entity_id = e_row.id
                AND t.attribute_id = '" attribute-id "'
            );
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;"))



(defn fn-entities-attribute-count
  "Used to get the count of all the attributes of a schema type.
   
   query Example {
      entities {
        id
        name
        attributeCount
      }
  }"
  []
  (str "CREATE OR REPLACE FUNCTION entities_attribute_count(e_row entities)
    RETURNS integer AS $$
    DECLARE
        attribute_count integer;
    BEGIN
        SELECT count(*)
        INTO attribute_count
        FROM entities_attributes(e_row);
        RETURN attribute_count;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;"))


(defn fn-entities-schema
  "Used to get the schema for an entity by looking up every type of the entity and then getting all of the attributes of those types.
   
   query Example {
      entities {
        id
        name
        schema {
          id
          name
        } 
      }
  }"
  []
  (str "CREATE OR REPLACE FUNCTION entities_schema(e_row entities)
    RETURNS SETOF entities AS $$
    BEGIN
        -- Using CTE to first fetch all types of the given entity
        RETURN QUERY 
        WITH entity_types AS (
            SELECT t.value_id AS type_id
            FROM triples t
            WHERE t.entity_id = e_row.id AND t.attribute_id = '" type-id "'
        ),
        type_attributes AS (
            -- For each type, fetch the associated attributes
            SELECT DISTINCT t.value_id AS attribute_id
            FROM entity_types et
            JOIN triples t ON t.entity_id = et.type_id AND t.attribute_id = '" attribute-id "'
        )
        SELECT e.*
        FROM entities e
        JOIN type_attributes ta ON e.id = ta.attribute_id;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;"))

(defn fn-parsed-attribute-values
  "The values for attributes but converted into their appropriate value type."
  []
  "
CREATE type primitive_value {
  value {
    id: text;
    type: text;
    value: text;
  }
};

CREATE type entity_value {
  value {
    id: text;
    type: 'entity';
    entityValue: public.entities;
  }
};

CREATE type unknown_value {
  value {
    id: text;
    type: text;
    value: text;
    entityValue: public.entities;
  }
}; 
 ")

(defn parse-pg-fn-name [type-name]
  (if (not (= (type type-name) java.lang.String))
    (println "Invalid type name" type-name)
    (-> (str type-name)
        s/lower-case
        (s/replace " " "_")
        (s/replace #"[^a-z_]" ""))))


(defn entity->attribute-fn
  [entity]

  (let [attribute-name (parse-pg-fn-name (:entities/name entity))
        value-type (:entities/value_type entity)
        attribute-ids (map str [(:entities/id entity)])]


    (str "CREATE OR REPLACE FUNCTION entities_" attribute-name "(e_row entities)
          RETURNS SETOF EntityAttribute AS $$
          BEGIN
            RETURN QUERY
            SELECT 'entity' AS type, e AS value
            FROM entities e
            WHERE e.id IN (
                SELECT t.value_id
                FROM triples t
                WHERE t.entity_id = e_row.id
                AND t.attribute_id IN (" (s/join ", " attribute-ids) ")
            );
          END;
          $$ LANGUAGE plpgsql STRICT STABLE;")))


(defn try-execute-raw-sql [raw-sql]
  (try-execute (sql/format [:raw raw-sql])))


(defn drop-all-pg-functions
  []
  "DROP FUNCTION IF EXISTS pg_catalog.pg_function CASCADE")

(defn populate-pg-functions
  "Prepares some type and schema GraphQL queries for Postgraphile"
  []
  (try-execute-raw-sql (drop-all-pg-functions))
  (try-execute-raw-sql (fn-entities-types))
  (try-execute-raw-sql (fn-entities-type-count))
  (try-execute-raw-sql (fn-entities-attributes))
  (try-execute-raw-sql (fn-entities-attribute-count))
  (try-execute-raw-sql (fn-entities-schema))
  (let [attribute-entities (get-all-attribute-entities)]
    (doseq [entity attribute-entities]
      (when (parse-pg-fn-name (:entities/name entity))
        (try-execute-raw-sql (entity->attribute-fn entity))))))
