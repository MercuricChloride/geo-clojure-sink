(ns lein-test.pg-function-helpers
  (:require [clojure.string :as s]
            [honey.sql :as sql]
            [lein-test.constants :refer [ATTRIBUTES ENTITIES]]
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



(defn parse-pg-fn-name [type-name]
  (when (and (string? type-name) (not (empty? type-name)))
    (-> type-name
        s/lower-case
        (s/replace " " "_")
        (s/replace #"[^a-z_]" "")
        (s/trim))))


(defn fn-parsed-attribute-values
  "The values for attributes but converted into their appropriate value type."
  []
  "
CREATE TYPE attribute_with_scalar_value_type AS (
    id text,
    type text,
    value text
);

CREATE TYPE attribute_with_relation_value_type AS (
    id text,
    type text, 
    entityValue public.entities 
);

CREATE TYPE attribute_with_no_value_type AS (
    id text,
    type text,
    value text,
    entityValue public.entities 
);
 ")


(defn classify-attribute-value-type [id]
  (cond
    (nil? id) "unknown"
    (= id (:id (:relation ENTITIES))) "relation"
    :else "primitive"))


(defn entity->attribute-relation-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
        DROP FUNCTION IF EXISTS entities_" attribute-name "(e_row entities);
        
        CREATE FUNCTION entities_" attribute-name "(e_row entities)
        RETURNS SETOF attribute_with_relation_value_type AS $$
        BEGIN
          RETURN QUERY
          SELECT 'entity' AS type, e AS entityValue
          FROM entities e
          WHERE e.id IN (
              SELECT t.value_id
              FROM triples t
              WHERE t.entity_id = e_row.id
              AND t.attribute_id IN (" quoted-ids ")
          );
        END;
        $$ LANGUAGE plpgsql STRICT STABLE;")))

(defn entity->attribute-scalar-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
        DROP FUNCTION IF EXISTS entities_" attribute-name "(e_row entities);
                                                           
        CREATE FUNCTION entities_" attribute-name "(e_row entities)
        RETURNS SETOF attribute_with_scalar_value_type AS $$
        BEGIN
          RETURN QUERY
          SELECT t.value_type AS type, t.string_value AS value
          FROM triples t
          WHERE t.entity_id = e_row.id
          AND t.attribute_id IN (" quoted-ids ")
          AND t.value_type IS NOT NULL;
        END;
        $$ LANGUAGE plpgsql STRICT STABLE;")))

(defn entity->attribute-no-value-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
        DROP FUNCTION IF EXISTS entities_" attribute-name "(e_row entities);
                                                           
        CREATE FUNCTION entities_" attribute-name "(e_row entities)
               RETURNS SETOF attribute_with_scalar_value_type AS $$
               BEGIN
                 RETURN QUERY
                 SELECT t.value_type AS type, t.string_value AS value
                 FROM triples t
                 WHERE t.entity_id = e_row.id
                 AND t.attribute_id IN (" quoted-ids ")
                 AND t.value_type IS NOT NULL
                                                                                    
                e AS entityValue
                FROM entities e
                WHERE e.id IN (
                  SELECT t.value_id
                  FROM triples t
                  WHERE t.entity_id = e_row.id
                  AND t.attribute_id IN (" quoted-ids "
                )
          );
                                                                                    ;
               END;
               $$ LANGUAGE plpgsql STRICT STABLE;")))


(defn entity->attribute-fn-wrapper
  [entity]
  (let [attribute-name (parse-pg-fn-name (:entities/name entity))
        value-type (classify-attribute-value-type (:entities/value_type entity))
        attribute-ids (map str [(:entities/id entity)])]

    (cond
      (= value-type "relation")
      (entity->attribute-relation-fn attribute-name attribute-ids)

      (nil? value-type)
      (entity->attribute-no-value-fn attribute-name attribute-ids)


      :else
      (entity->attribute-relation-fn attribute-name attribute-ids))))

(defn try-execute-raw-sql [raw-sql]
  (try-execute (sql/format [:raw raw-sql])))


(defn drop-pg-functions+types
  []
  "
   DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.proname || ' CASCADE';
    END LOOP;
END $$;
   ")


(defn populate-pg-functions
  "Prepares some type and schema GraphQL queries for Postgraphile"
  []
  (try-execute-raw-sql (drop-pg-functions+types))
  (try-execute-raw-sql (fn-entities-types))
  (try-execute-raw-sql (fn-entities-type-count))
  (try-execute-raw-sql (fn-entities-attributes))
  (try-execute-raw-sql (fn-entities-attribute-count))
  (try-execute-raw-sql (fn-entities-schema))
  ;; (try-execute-raw-sql (fn-parsed-attribute-values))
  (let [attribute-entities (get-all-attribute-entities)]
    (doseq [entity attribute-entities]
      (when (parse-pg-fn-name (:entities/name entity))
        (try-execute-raw-sql (entity->attribute-fn-wrapper entity))))))
    
  
