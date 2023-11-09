(ns lein-test.spec.action
  (:gen-class)
  (:require [clojure.spec.alpha :as s]))

(defn action? [input]
  (#{"createTriple" "deleteTriple" "createEntity"} input))

(defn value-type? [input]
  (#{"string" "entity"} input))

; spec def for the action triple
(s/def :action/type action?)
(s/def :action/entityId string?)
(s/def :action/attributeId string?)
(s/def :action/proposal-name (s/nilable string?))
(s/def :value/type value-type?)
(s/def :value/id string?)
(s/def :value/value string?)

(s/def :action/value
  (s/keys :req-un [:value/type :value/id] :opt-un [:value/value]))

(s/def ::action-triple
  (s/keys :req-un [:action/type
                   :action/entityId
                   :action/attributeId
                   :action/value]))

(s/def ::action-with-proposal-name
  (s/keys :req-un [:action/type
                   :action/entityId
                   :action/attributeId
                   :action/value
                   :action/proposal-name]))

(s/def ::triples-with-proposal-names
  (s/coll-of ::action-with-proposal-name))

(defn valid-action? [action]
  (s/valid? ::action-triple action))

; spec def for the actions table in the db
(s/def :action/entity string?)
(s/def :action/attribute string?)
(s/def :action/version-id string?)
(s/def :action/proposed-version-id string?)
(s/def :action/value-type string?)
(s/def :action/value-id string?)

(s/def ::action-table-entry
  (s/keys :req-un [:action/entity :action/attribute]
          :opt-un [:action/value-id :action/value-type :action/proposed-version-id :action/version-id]))

(s/def ::action-table-entries
  (s/coll-of ::action-table-entry))

(def test-triple {:type "createTriple"
                  :entityId "foo"
                  :attributeId "space"
                  :value {:type "string"
                          :id "string-entity"
                          :value "Some string"}})
