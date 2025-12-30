(ns lima.cli
  (:require [cli-matic.core :as cli-matic]
            [lima.adapter.beanfile :as beanfile]
            [lima.adapter.tabulate :as tabulate]
            [lima.core.inventory :as inv]
            [lima.adapter.import :as import]))

(defn report
  "Run the named report"
  [{:keys [name beanpath]}]
  (case name
    "count" (let [{:keys [directives options]} (beanfile/book beanpath)
                  inv (inv/build directives options)
                  tab (tabulate/inventory inv)]
              (println tab))))

(defn import-files
  "Import files"
  [{config-path :config beanpath :context import-paths  :_arguments}]
  (let [config (if config-path (import/read-config config-path) import/DEFAULT-CONFIG)
        digest (and context (beanfile/digest context))
        import-paths _arguments
        classified (mapv (fn [import-path] (import/classify config import-path)) import-paths)
        ingested (import/)]
    (println "import" imports "with digest" digest)
    (import)
    ))

(def CONFIGURATION
  {:command "lima",
   :description "A new implementation of Beancount in Clojure/Rust",
   :version "0.0.1",
   :subcommands [{:command "report",
                  :description "Run a canned Lima report",
                  :opts [{:as "Name",
                          :option "name",
                          :short 0,
                          :type #{"count"},
                          :default "count"}
                         {:as "Beancount file path",
                          :option "beanpath",
                          :short 1,
                          :type :string,
                          :env "LIMA_BEANPATH",
                          :default :present}],
                  :runs report}
                 {:command "import",
                  :description "Import various format files into Beancount",
                  :opts [{:as "Beancount file path for import context",
                          :option "context",
                          :type :string,
                          :env "LIMA_BEANPATH"}
                         {:as "Import config path",
                          :option "config",
                          :type :string,
                          :env "LIMA_IMPORT_CONFIG"
                          }],
                  :runs import-files}]})
