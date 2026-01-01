(ns lima.harvest.cli
  (:require [cli-matic.core :as cli-matic]
            [lima.harvest.adapter.beanfile :as beanfile]
            [lima.harvest.adapter.harvest :as adapter]))

(defn harvest
  "Harvest files for import"
  [{config-path :config, beanpath :context, import-paths :_arguments}]
  (let [config (if config-path
                 (adapter/read-config config-path)
                 adapter/DEFAULT-CONFIG)
        digest (if beanpath (beanfile/digest beanpath) beanfile/EMPTY-DIGEST)]
    (adapter/harvest config digest import-paths)))

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
