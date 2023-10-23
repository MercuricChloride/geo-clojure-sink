(ns lein-test.spec.action
  (:require [clojure.spec.alpha :as s]))

(defn action? [input]
  (#{"createTriple" "deleteTriple" "createEntity"} input))

(defn value-type? [input]
  (#{"string" "entity"} input))

(s/def :action/type action?)
(s/def :action/entityId string?)
(s/def :action/attributeId string?)
(s/def :value/type value-type?)
(s/def :value/id string?)
(s/def :value/value string?)

(s/def ::value
  (s/keys :req-un [:value/type :value/id] :opt-un [:value/value]))

(s/def ::actionTriple
  (s/keys :req-un [
                   :action/type
                   :action/entityId
                   :action/attributeId
                   ::value]))

(defn valid-action? [action]
  (s/valid? ::actionTriple action))

(def test-triple {
           :type "createTriple"
           :entityId "foo"
           :attributeId "space"
           :value {
                   :type "string"
                   :id "string-entity"
                   :value "Some string"
                   }
           })

(valid-action? test-triple)
