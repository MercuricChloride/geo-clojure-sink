(ns lein-test.core
  (:gen-class)
  (:require [cheshire.core :as ch]
            [clojure.java.io :as io]
            [clojure.string :as cstr]
            [geo.clojure.sink :as geo]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [lein-test.constants :refer [ATTRIBUTES ENTITIES]]
            [lein-test.db-helpers :refer [try-execute]]
            [lein-test.spec.action :as action]
            [lein-test.substreams :as substreams]
            [lein-test.tables :refer [->action ->entity ->spaces ->triple]]))


(defn- validate-actions
  ([actions]
   (filter #(action/valid-action? %) actions))
  ([space author block-number actions]
   (->> (filter #(action/valid-action? %) actions)
        (map #(assoc % :space space :author author :block-number block-number)))))

(defn- json->actions
  ([path]
   (let [json (slurp path)]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path]
   (let [json (slurp (str prefix path))]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path space author block-number]
   (let [json (slurp (str prefix path))]
     (->> (ch/parse-string json true)
          :actions
          (validate-actions space author block-number)))))

(defn- extract-file-meta [filename]
  (let [parts (cstr/split filename #"_")]
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (get parts 3)
     :filename filename}))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(def new-files (->> (io/file "./new-cache/entries-added/")
                    file-seq
                    rest
                    (map #(geo/pb->EntryAdded (substreams/slurp-bytes %)))))

(def roles-granted (->> (io/file "./new-cache/roles-granted/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleGranted (substreams/slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

(def roles-revoked (->> (io/file "./new-cache/roles-revoked/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleRevoked (substreams/slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

(def files (->> (io/file "./action_cache/")
                file-seq
                rest
                (map #(cstr/replace % #"./action_cache/" ""))
                (map extract-file-meta)
                sort-files
                (map #(json->actions "./action_cache/" (:filename %) (:space %) (:author %) (:block %)))))

(defn populate-entities
  "Takes in a seq of actions and populates the `entities` table"
  [actions]
  (let [formatted-sql (-> (h/insert-into :public/entities)
                          (h/values (into [] (map ->entity actions)))
                          (h/on-conflict :id (h/do-nothing))
                          (sql/format {:pretty true})
                          try-execute)]
    (println "Generated SQL:" formatted-sql)
    formatted-sql))
    


(defn populate-triples
  "Takes in a seq of actions and populates the `triples` table"
  [actions]
  (-> (h/insert-into :public/triples)
      (h/values (map ->triple actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-actions
  "Takes in a seq of actions and populates the `actions` table"
  [actions]
  (-> (h/insert-into :public/actions)
      (h/values (into [] actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-spaces [actions]
  (let [filtered (filter #(= (:attributeId %) "space") actions)]
    (when (< 0 (count filtered))
        (-> (h/insert-into :spaces)
            (h/values (into [] (map ->spaces filtered)))
            (h/on-conflict :id (h/do-nothing))
            (sql/format {:pretty true})
            try-execute))))

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

(defn entry->actions
  [entry entity-id]
  (filter #(= (:entityId %) entity-id) entry))

(defn ->proposed-version+actions
  [action-map created-at-block timestamp author proposal-id]
  (let [proposed-version-id (str (java.util.UUID/randomUUID))]
    [{:id proposed-version-id
      :name "update-me" ;TODO UPDATE THIS
      :description "update-me" ;TODO UPDATE THIS
      :created-at timestamp
      :created-at-block created-at-block
      :created-by author
      :entity (:entityId (first action-map))
      :proposal-id proposal-id}
      
     (map #(->action % proposed-version-id) action-map)]))
    
  

(defn new-proposal
  [created-at-block timestamp author proposal-id space]
  {:id proposal-id
   :name "update-me"
   :description "update-me"
   :created-at timestamp
   :created-at-block created-at-block
   :created-by author
   :space space
   :status "APPROVED"}) ;NOTE THIS IS HARDCODED FOR NOW UNTIL GOVERNANCE FINALIZED
   

(defn populate-proposal
  [proposal]
  (-> (h/insert-into :public/proposals)
      (h/values [proposal])
      (h/on-conflict :id (h/do-nothing))
      (sql/format)
      try-execute))

(defn populate-proposed-version
  [proposed-version]
  (-> (h/insert-into :public/proposed-versions)
      (h/values [proposed-version])
      (h/on-conflict :id (h/do-nothing))
      (sql/format)
      try-execute))

(defn update-entity
  "Updates a specific entity in the table."
  [entityId column value]
  (try-execute (-> (h/update :public/entities)
                   (h/set {column value})
                   (h/where [:= :id entityId])
                   (sql/format {:pretty true}))))

(defn populate-columns
  "Takes actions as arguments, processes them to find blessed columns to update and updates them."
  [actions]
  (let [name-attr-id (:id (:name ATTRIBUTES))
        description-attr-id (:id (:description ATTRIBUTES))
        value-type-attr-id (:id (:value-type ATTRIBUTES))
        type (:id (:type ATTRIBUTES))
        attribute (:id (:attribute ENTITIES))
        schema-type (:id (:schema-type ENTITIES))]
    (doseq [action actions]
      (let [triple (->triple action)
            is-name-update (and (= (:attribute_id triple) name-attr-id)
                                (= (:value_type triple) "string"))
            is-description-update (and (= (:attribute_id triple) description-attr-id)
                                       (= (:value_type triple) "string"))
            is-value-type-update (and (= (:attribute_id triple) value-type-attr-id)
                                      (= (:value_type triple) "entity"))
            is-type-flag-update (and (= (:attribute_id triple) type)
                                     (= (:value_id triple) schema-type))
            is-attribute-flag-update (and (= (:attribute_id triple) type)
                                          (= (:value_id triple) attribute))
            action-type (:type action)
            is-delete-triple (= action-type "deleteTriple")]

        ;; TODO: Add Space and Account Updates
        (when is-name-update
          (update-entity (:entity_id triple) :name (if is-delete-triple nil (:string_value triple))))
        (when is-description-update
          (update-entity (:entity_id triple) :description (if is-delete-triple nil (:string_value triple))))
        (when is-value-type-update
          (update-entity (:entity_id triple) :attribute_value_type_id (if is-delete-triple nil (:value_id triple))))
        (when is-type-flag-update
          (update-entity (:entity_id triple) :is_type (if is-delete-triple nil (boolean (:value_id triple)))))
        (when is-attribute-flag-update
          (update-entity (:entity_id triple) :is_attribute (if is-delete-triple nil (boolean (:value_id triple)))))))))

(defn entry->proposal
  [log-entry]
  (let [first-entry (first log-entry)
        author (:author first-entry) ;NOTE the author will be the same for all triples in an action
        space (:space first-entry)
        block-number (:block-number first-entry)
        proposal-id (str (java.util.UUID/randomUUID))
        entity-ids (into #{} (map :entityId log-entry))
        action-map (map #(entry->actions log-entry %) entity-ids)
        proposal (new-proposal block-number block-number author proposal-id space)
        proposed-versions+actions (map #(->proposed-version+actions % block-number block-number author proposal-id) action-map)]
    [proposal proposed-versions+actions]))

(defn- populate-proposed-version+actions
  [input]
  (let [[proposed-version actions] input]
    (populate-proposed-version proposed-version)
    (populate-actions actions)))

(defn populate-proposals-from-entry
  [log-entry]
  (let [[proposal proposed-version+actions] (entry->proposal log-entry)]
    (populate-proposal proposal)
    (map populate-proposed-version+actions proposed-version+actions)))

(defn- entry->author
  [entry]
  (->> entry
     first
     :author))

(defn populate-account
  [log-entry]
  (let [account (entry->author log-entry)]
    (when (not (nil? account))
      (-> (h/insert-into :public/accounts)
          (h/values [{:id account}])
          (h/on-conflict :id (h/do-nothing))
          (sql/format)
          try-execute))))

(defn populate-db [type log-entry]
  (cond (= type :entities) (populate-entities log-entry)
        (= type :triples) (populate-triples log-entry)
        (= type :accounts) (populate-account log-entry)
        (= type :spaces) (populate-spaces log-entry)
        (= type :columns) (populate-columns log-entry)
        (= type :proposals) (populate-proposals-from-entry log-entry)
        :else (throw (ex-info "Invalid type" {:type type}))))

(def start-block 36472424)
(def stop-block 48000000)

(defn handle-args
  [args]
  (let [args (into #{} args)
        from-genesis (get args "--from-genesis")
        populate-cache (get args "--populate-cache")
        from-cache (get args "--from-cache")]
    (when from-genesis
      (println "from-genesis")
      (swap! substreams/current-block (fn [_] (str start-block)))
      (swap! substreams/cursor (fn [_] "")))
    (when populate-cache
      (println "populate-cache")
      (swap! substreams/sink-mode (fn [_] :populate-cache)))
    (when from-cache
      (println "from-cache")
      (swap! substreams/sink-mode (fn [_] :from-cache)))))


(defn -main
  "The main enchilada that runs when you write lein run"
  [& args]
  (handle-args args)

  (while true
    (println "Starting stream at block #" (str @substreams/current-block))
    (let [client (substreams/spawn-client)]
      (substreams/start-stream client start-block stop-block))))
