(ns lein-test.pg-function-helpers 
  (:require [clojure.string :as cstr]
            [lein-test.constants :as c]))

(def type-id (c/ATTRIBUTES :type :id))
(def attribute-id (c/ATTRIBUTES :attribute :id))


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
    "CREATE OR REPLACE FUNCTION entities_attributes(e_row entities)
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
    $$ LANGUAGE plpgsql STRICT STABLE;")

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
  "CREATE OR REPLACE FUNCTION entities_schema(e_row entities)
    RETURNS SETOF entities AS $$
    BEGIN
        -- Using CTE to first fetch all types of the given entity
        RETURN QUERY 
        WITH entity_types AS (
            SELECT t.value_id AS type_id
            FROM triples t
            WHERE t.entity_id = e_row.id AND t.attribute_id = 'type'
        ),
        type_attributes AS (
            -- For each type, fetch the associated attributes
            SELECT DISTINCT t.value_id AS attribute_id
            FROM entity_types et
            JOIN triples t ON t.entity_id = et.type_id AND t.attribute_id = '01412f83-8189-4ab1-8365-65c7fd358cc1'
        )
        SELECT e.*
        FROM entities e
        JOIN type_attributes ta ON e.id = ta.attribute_id;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;")


(def template-function-str
    " CREATE OR REPLACE FUNCTION \"type-$$ENTITY_ID$$ \" (ent_id text)
RETURNS public.entity_types AS $$
DECLARE
result_record public.entity_types;
BEGIN
SELECT *
INTO result_record
FROM entity_types
WHERE type = '$$ENTITY_ID$$'
AND entity_id = ent_id;

RETURN result_record;
EXCEPTION
WHEN NO_DATA_FOUND THEN
RETURN NULL;
END;
$$ LANGUAGE plpgsql STRICT STABLE;
comment on function \"type-$$ENTITY_ID$$ \" (_entity_id text) is E' @name $$ENTITY_NAME$$';
")

(defn type-function
  " Creates a function in the DB to get all entities of a given type "
  [entity-id entity-name]
  (-> template-function-str
    (cstr/replace " $$ENTITY_ID$$ " entity-id)
    (cstr/replace " $$ENTITY_NAME$$ " entity-name)))

(def all-type-function
    " CREATE OR REPLACE FUNCTION allTypes ()
RETURNS SETOF entities AS $$
BEGIN
RETURN QUERY
SELECT e.*
FROM entities e
WHERE e.is_type = true;
END;
$$ LANGUAGE plpgsql STRICT STABLE;")

(def all-attribute-function
  "CREATE OR REPLACE FUNCTION entities_attributes(e entities)
    RETURNS SETOF entities AS $$
    BEGIN
    RETURN QUERY
    SELECT e.*
    FROM entities
    JOIN triples t ON entities.id = t.entity_id
    WHERE t.attribute_id = '01412f83-8189-4ab1-8365-65c7fd358cc1' AND e.id = t.value_id;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;")
