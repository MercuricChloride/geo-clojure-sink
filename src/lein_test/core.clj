(ns lein-test.core
  (:gen-class)
  (:require
   [cheshire.core :as ch]
   [lein-test.constants :refer [ENTITIES]]
   [clojure.string :as cstr]
   [clojure.java.io :as io]
   [lein-test.spec.action :as action]
   [honey.sql :as sql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [lein-test.tables :refer [->action ->triple ->entity ->entity-type ->entity-attribute ->spaces]]
   [lein-test.db-helpers :refer [nuke-db try-execute create-type-tables make-space-schemas]]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(defn- validate-actions
  ([actions]
   (filter #(action/valid-action? %) actions))
  ([space author actions]
   (->> (filter #(action/valid-action? %) actions)
        (map #(assoc % :space space :author author)))))

(defn- json->actions
  ([path]
   (let [json (slurp path)]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path]
   (let [json (slurp (str prefix path))]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path space author]
   (let [json (slurp (str prefix path))]
     (->> (ch/parse-string json true)
          :actions
          (validate-actions space author)))))

(defn- extract-file-meta [filename]
  (let [parts (cstr/split filename #"_")]
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (-> (get parts 3)
                 (cstr/replace #"\.json" ""))
     :filename filename}))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(def files (->> (io/file "./action_cache/")
                file-seq
                rest
                (map #(cstr/replace % #"./action_cache/" ""))
                (map extract-file-meta)
                sort-files
                (map #(json->actions "./action_cache/" (:filename %) (:space %) (:author %)))))

(defn populate-entities
  "Takes in a seq of actions and populates the `entities` table"
  [actions]
  (-> (h/insert-into :entities)
      (h/values (into [] (map ->entity actions)))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-triples
  "Takes in a seq of actions and populates the `triples` table"
  [actions]
  (-> (h/insert-into :triples)
      (h/values (map ->triple actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-actions
  "Takes in a seq of actions and populates the `actions` table"
  [actions]
  (-> (h/insert-into :actions)
      (h/values (into [] (map ->action actions)))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-types
  "Takes in a seq of actions and populates the `entity_types` table"
  [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "type") actions))]
    (-> (h/insert-into :entity_types)
        (h/values (into [] (map ->entity-type filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))

(defn populate-attributes [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "01412f83-8189-4ab1-8365-65c7fd358cc1") actions))]
    (-> (h/insert-into :entity_attributes)
        (h/values (into [] (map ->entity-attribute filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))

(defn populate-spaces [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "space") actions))]
    (-> (h/insert-into :spaces)
        (h/values (into [] (map ->spaces filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))

(defn populate-db [type actions]
  (cond (= type :entities) (populate-entities actions)
        (= type :triples) (populate-triples actions)
        (= type :actions) (populate-actions actions)
        (= type :types) (populate-types actions)
        (= type :attributes) (populate-attributes actions)
        (= type :spaces) (populate-spaces actions)
        :else (throw (ex-info "Invalid type" {:type type}))))

;; (try-execute
;;  (sql/format [:raw "CREATE INDEX idx_entity_attribute ON public.triples(entity_id, attribute_id);"]))
;; (try-execute
;;  (sql/format [:raw "CREATE INDEX idx_entity_attribute_value_id ON public.triples(entity_id, attribute_id, value_id);"]))

;(h/add-index :idx-entity-attribute)
;(h/alter)

;(nuke-db)

 ;; (time
 ;;  (do
 ;;    (time (doall (map #(populate-db :entities %) files)))
 ;;    (time (doall (map #(populate-db :triples %) files)))
 ;;    (time (doall (map #(populate-db :spaces %) files)))
 ;;    (time (doall (map #(populate-db :types %) files)))
 ;;    (time (doall (map #(populate-db :attributes %) files)))
 ;;    (time (make-space-schemas))
 ;;    (time (create-type-tables))
 ;;    (println "done with everything")))

(def template-function-str
    "CREATE OR REPLACE FUNCTION \"type-$$ENTITY_ID$$\"(ent_id text)
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
    comment on function \"type-$$ENTITY_ID$$\"(_entity_id text) is E'@name $$ENTITY_NAME$$';
    ")

(defn type-function
  "Creates a function in the DB to get all entities of a given type"
  [entity-id entity-name]
  (-> template-function-str
    (cstr/replace "$$ENTITY_ID$$" entity-id)
    (cstr/replace "$$ENTITY_NAME$$" entity-name)))

(def all-type-function
    "CREATE OR REPLACE FUNCTION allTypes()
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

;; (try-execute (sql/format [:raw all-type-function]))
;; (try-execute (sql/format [:raw all-attribute-function]))
;; (try-execute (sql/format [:raw (type-function "type" "types")]))
