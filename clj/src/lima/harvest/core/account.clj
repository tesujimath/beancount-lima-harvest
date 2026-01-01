(ns lima.harvest.core.account
  (:require [clojure.string :as str]))

(defn infer-from-path
  [digest classified]
  (let [accids (or (and digest (:accids digest)) {})
        path (:path classified)
        matching (filterv #(str/includes? path %) (keys accids))]
    (case (count matching)
      0 (throw (Exception. (str/join " " ["no accid matches" path])))
      1 (update classified :hdr #(assoc % :accid (first matching)))
      (throw (Exception. (str/join " "
                                   (concat ["multiple accids match" path ":"]
                                           matching)))))))
