(ns lein-test.pg-function-helpers 
  (:require [honey.sql :as sql]
            [lein-test.constants :refer [ATTRIBUTES]]
            [lein-test.db-helpers :refer [try-execute]]))

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


(defn execute-base-pg-fns
  "Executes our functions for Postgraphile to pickup"
  []
  (try-execute (sql/format [:raw (fn-entities-types)]))
  (try-execute (sql/format [:raw (fn-entities-type-count)]))
  (try-execute (sql/format [:raw (fn-entities-attributes)]))
  (try-execute (sql/format [:raw (fn-entities-attribute-count)]))
  (try-execute (sql/format [:raw (fn-entities-schema)]))
  )



