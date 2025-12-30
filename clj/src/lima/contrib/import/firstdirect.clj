(ns lima.contrib.import.firstdirect (require [clojure.string :as str]))

(defn payee-narration
  "Extract payee and narration from firstdirect hdr and txn.

  The description field is a composite of payee and narration with spaces between,
  so we attempt to split on two or more spaces, and if we can't just take it as narration.
  "
  [hdr txn]
  (if-let [description (get txn "description")]
    (let [[s1 s2] (str/split description #"   *" 2)]
      (if s2 {:payee s1, :narration s2} {:narration s1}))
    {}))
