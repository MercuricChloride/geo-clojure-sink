(ns lein-test.utils
  (:require
   [clojure.java.io :as io]))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [in (io/input-stream x)
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn write-file [path input]
  (with-open [o (io/output-stream path)]
    (.write o input)))
