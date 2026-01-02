(ns lima.harvest.cli
  (:require [cli-matic.core :as cli-matic]
            [lima.harvest.adapter.beanfile :as beanfile]
            [lima.harvest.adapter.harvest :as adapter]
            [failjure.core :as f]))

(defn harvest
  "Harvest files for import"
  [{config-path :config, beanpath :context, import-paths :_arguments}]
  (let [config (if config-path
                 (adapter/read-config config-path)
                 adapter/DEFAULT-CONFIG)]
    (f/attempt-all
      [digest (if beanpath (beanfile/digest beanpath) beanfile/EMPTY-DIGEST)
       harvested (adapter/harvest-all config digest import-paths)]
      harvested
      (f/when-failed [e] (do (println (f/message e) *err*) (System/exit 1))))))

(def CONFIGURATION
  {:command "lima-harvest",
   :version "0.0.1",
   :description "Import various format files into Beancount",
   :opts [{:as "Beancount file path for import context",
           :option "context",
           :type :string,
           :env "LIMA_BEANPATH"}
          {:as "Import config path",
           :option "config",
           :type :string,
           :env "LIMA_HARVEST_CONFIG"}],
   :runs harvest})
