(ns lima.harvest.core.account
  (:require [clojure.string :as str]
            [failjure.core :as f]))

(defn infer-from-path
  [digest classified]
  (let [accids (or (and digest (:accids digest)) {})
        path (:path classified)
        matching (filterv #(str/includes? path %) (keys accids))]
    (case (count matching)
      0 (f/fail "infer-from-path failed - no accid matches %s" path)
      1 (update classified :hdr #(assoc % :accid (first matching)))
      (f/fail "multiple accids match %s: " path (str/join " " matching)))))
