(ns lima.harvest.adapter.config
  (:require [clojure.edn :as edn]
            [malli.core :as m]
            [lima.harvest.core.config :refer [Config]]
            [lima.harvest.core.malli-support :refer [validate-or-fail]]))

(defn read-from-file
  "Read harvest config from EDN file"
  [config-path]
  (let [raw (slurp config-path)
        c (assoc (edn/read-string raw) :path config-path)]
    (validate-or-fail Config c config-path)))
