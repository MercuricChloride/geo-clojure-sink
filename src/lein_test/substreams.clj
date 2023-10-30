(ns lein-test.substreams
  (:gen-class))
  ;; (:gen-class)
  ;; (:require
  ;;  [cheshire.core :as ch]
  ;;  [lein-test.constants :refer [ENTITIES]]
  ;;  [clojure.string :as cstr]
  ;;  [clojure.java.io :as io]
  ;;  [lein-test.spec.action :as action]
  ;;  [honey.sql :as sql]
  ;;  [honey.sql.helpers :as h]
  ;;  [next.jdbc :as jdbc]
  ;;  [lein-test.tables :refer [->action ->triple ->entity ->entity-type ->entity-attribute ->spaces]]
  ;;  [lein-test.db-helpers :refer [nuke-db try-execute create-type-tables make-space-schemas]
  ;;  [sf.substreams.rpc.v2 :as rpc]]))

;; Our grpc connection must pass in the authorization key with the value of our "token" r.metadata_mut().insert("authorization", t.clone());
;; :headers {"Content-Type" "application/grpc-web-text"}
