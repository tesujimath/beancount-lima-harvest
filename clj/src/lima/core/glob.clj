(ns lima.core.glob
  (:import [java.nio.file FileSystems Paths]))

(defn match?
  [glob path]
  (let [matcher (.getPathMatcher (FileSystems/getDefault) (str "glob:" glob))]
    (.matches matcher (Paths/get path (make-array String 0)))))
