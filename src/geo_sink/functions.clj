(ns geo-sink.functions
  (:require [clojure.string :as s]
            [geo-sink.constants :refer [ATTRIBUTES ENTITIES]]
            [geo-sink.db-helpers :refer [get-all-attribute-entities
                                         get-all-type-entities try-execute]]
            [honey.sql :as sql]))

(def type-id (str (:id (:type ATTRIBUTES))))

(defn entity->type-fn
  [type-name type-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") type-ids))]
    (str "
        DROP FUNCTION IF EXISTS public." type-name "();
        
        CREATE FUNCTION public." type-name "()
        RETURNS SETOF public.entities AS $$
        BEGIN
          RETURN QUERY
          SELECT *
          FROM public.entities e
          WHERE e.id IN (
              SELECT t.entity_id
              FROM triples t
              WHERE t.attribute_id = " type-id "  
              AND t.value_id IN (" quoted-ids ")
          );
        END;
        $$ LANGUAGE plpgsql STRICT STABLE;")))


(def protected-attribute-names
  ["name" "description" "entity_id" "is_type" "defined_in" "value_type_id" "version" "id"])

(defn parse-pg-fn-name [fn-name]
  (if (and (string? fn-name) (seq fn-name))
    (let [parsed-name (-> fn-name
                          s/trim
                          s/lower-case
                          (s/replace " " "_")
                          (s/replace #"[^a-z_]" ""))]
      (if (some #{parsed-name} protected-attribute-names)
        nil
        parsed-name))
    nil))


(defn classify-attribute-value-type [id]
  (cond
    (nil? id) "unknown"
    (= id (:id (:relation ENTITIES))) "relation"
    :else "scalar"))


(defn entity->attribute-relation-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
        DROP FUNCTION IF EXISTS public.entities_" attribute-name "(e_row public.entities);
        
        CREATE FUNCTION public.entities_" attribute-name "(e_row public.entities)
        RETURNS SETOF attribute_with_relation_value_type AS $$
        BEGIN
          RETURN QUERY
          SELECT t.value_type AS type, t.entity_id AS entity_value_id
          FROM public.triples t
          WHERE t.entity_id = e_row.id
          AND t.attribute_id IN (" quoted-ids ")
          AND t.value_type IS NOT NULL;
        END;
        $$ LANGUAGE plpgsql STRICT STABLE;")))

(defn entity->attribute-scalar-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
        DROP FUNCTION IF EXISTS public.entities_" attribute-name "(e_row public.entities);
                                                           
        CREATE FUNCTION public.entities_" attribute-name "(e_row public.entities)
        RETURNS SETOF attribute_with_scalar_value_type AS $$
        BEGIN
          RETURN QUERY
          SELECT t.value_type AS type, t.string_value AS value
          FROM public.triples t
          WHERE t.entity_id = e_row.id
          AND t.attribute_id IN (" quoted-ids ")
          AND t.value_type IS NOT NULL;
        END;
        $$ LANGUAGE plpgsql STRICT STABLE;")))

(defn entity->attribute-unknown-value-fn
  [attribute-name attribute-ids]
  (let [quoted-ids (clojure.string/join ", " (map #(str "'" % "'") attribute-ids))]
    (str "
      DROP FUNCTION IF EXISTS public.entities_" attribute-name "(e_row public.entities);
        
      CREATE FUNCTION public.entities_" attribute-name "(e_row public.entities)
      RETURNS SETOF attribute_with_unknown_value_type AS $$
      BEGIN
          RETURN QUERY
          SELECT t.value_type AS type, t.string_value AS value, t.value_id AS entity_value_id
          FROM public.triples t
          WHERE t.entity_id = e_row.id
          AND t.attribute_id IN (" quoted-ids ")
          AND t.value_type IS NOT NULL;
      END; 
      $$ LANGUAGE plpgsql STRICT STABLE;")))


(defn entity-name->attribute-count
  [name]
  (str "
        DROP FUNCTION IF EXISTS public.entities_" name "_count(e_row public.entities);
                                                           
        CREATE FUNCTION public.entities_" name "_count(e_row public.entities)
         RETURNS integer AS $$
    DECLARE
        attribute_count integer;
    BEGIN
        SELECT count(*)
        INTO attribute_count
        FROM public.entities_" name "(e_row);
        RETURN attribute_count;
    END;
  $$ LANGUAGE plpgsql STRICT STABLE;"))


(defn entities->attribute-fn
  [name entities]
  (let [value-types (map #(classify-attribute-value-type (:entities/attribute_value_type_id %)) entities)
        attribute-ids (map #(str (:entities/id %)) entities)
        all-relations? (every? #(= "relation" %) value-types)
        all-scalar? (every? #(= "scalar" %) value-types)]


    ;; (println "Debuging function value types: " name value-types)

    (cond
      all-relations?
      (entity->attribute-relation-fn name attribute-ids)

      all-scalar?
      (entity->attribute-scalar-fn name attribute-ids)

      :else
      (entity->attribute-unknown-value-fn name attribute-ids))))


(defn try-execute-raw-sql [raw-sql]
  (try-execute (sql/format [:raw raw-sql])))

(defn populate-pg-functions
  "Prepares some type and schema GraphQL queries for Postgraphile"
  []

  (let [type-entities (->> (get-all-type-entities)
                           (group-by (fn [entity] (parse-pg-fn-name (:entities/name entity))))
                           (into {}))]
    (doseq [[name entities] type-entities]
      (when (and (not (nil? name)) (not (= "" name)))
        (try-execute-raw-sql (entity->type-fn name (map :entities/id entities))))))

  (let [attribute-entities (->> (get-all-attribute-entities)
                                (group-by (fn [entity] (parse-pg-fn-name (:entities/name entity))))
                                (into {}))]
    (doseq [[name entities] attribute-entities]
      (when (and (not (nil? name)) (not (= "" name)))
        (try-execute-raw-sql (entities->attribute-fn name entities)))
      (try-execute-raw-sql (entity-name->attribute-count name)))))
    
  
