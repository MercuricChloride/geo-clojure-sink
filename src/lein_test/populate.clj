(ns lein-test.populate
 (:require
  [honey.sql :as sql]
  [honey.sql.helpers :as h]
  [lein-test.cache :refer [cached-actions cached-entries]]
  [lein-test.constants :refer [ATTRIBUTES ENTITIES]]
  [lein-test.db-helpers :refer [try-execute]]
  [lein-test.tables :refer [->action ->entity ->spaces ->triple]]))

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

(defn populate-spaces
  [actions]
  (let [filtered (filter #(= (:attributeId %) "space") actions)]
    (when (< 0 (count filtered))
        (-> (h/insert-into :public/spaces)
            (h/values (into [] (map ->spaces filtered)))
            (h/on-conflict :id (h/do-nothing))
            (sql/format {:pretty true})
            try-execute))))

(defn entry->actions
  [entry entity-id]
  (filter #(= (:entityId %) entity-id) entry))

(defn ->proposed-version+actions
  [action-map created-at-block timestamp author proposal-id proposal-name]
  (let [proposed-version-id (str (java.util.UUID/randomUUID))]
    [{:id proposed-version-id
      :name proposal-name
      ;:description nil ;TODO Eventually this should have a value
      :created-at timestamp
      :created-at-block created-at-block
      :created-by author
      :entity (:entityId (first action-map))
      :proposal-id proposal-id}
     (map #(->action % proposed-version-id) action-map)]))



(defn new-proposal
  [created-at-block timestamp author proposal-id space proposal-name]
  {:id proposal-id
   :name proposal-name
   :description nil
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
        proposal-name (:name log-entry)
        proposal (new-proposal block-number block-number author proposal-id space proposal-name)
        proposed-versions+actions (map #(->proposed-version+actions % block-number block-number author proposal-id proposal-name) action-map)]
    [proposal proposed-versions+actions]))

(defn- populate-proposed-version+actions
  [input]
  (let [[proposed-version actions] input]
    (populate-proposed-version proposed-version)
    (populate-actions actions)))

(defn populate-proposals
  [log-entry]
  (let [[proposal proposed-version+actions] (entry->proposal log-entry)]
    (println "PROPOSAL: " proposal)
    (println "OTHER THING: " proposed-version+actions)
    (populate-proposal proposal)
    (map populate-proposed-version+actions proposed-version+actions)))

(defn populate-account
  [actions]
  (let [account (:author (first actions))]
    (when (not (nil? account))
      (-> (h/insert-into :public/accounts)
          (h/values [{:id account}])
          (h/on-conflict :id (h/do-nothing))
          sql/format
          try-execute))))

(defn log-entry->db
 [actions]
 (populate-entities actions)
 (populate-triples actions)
 (populate-account actions)
 (populate-spaces actions)
 (populate-columns actions)
 (populate-proposals actions))

(let [thangs (take 10 cached-actions)]
   (populate-proposals (first thangs)))

(defn roles-granted->db [roles-granted]
 []
 "hi")

(defn roles-revoked->db [roles-revoked]
 []
 "hi again")

