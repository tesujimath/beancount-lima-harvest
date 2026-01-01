(ns lima.harvest.core.infer)

(defn secondary-accounts
  [payees narrations]
  (fn [txn]
    (let [units (or (:units txn) 0M)
          primary-acc (:acc txn)
          found-payee (get payees (:payee txn))
          found-narration (get narrations (:narration txn))
          order-accounts
            (fn [acc-count category]
              (let [all-account-names (keys acc-count)
                    candidate-account-names (filterv #(not= % primary-acc)
                                              all-account-names)
                    annotated-accounts (mapv (fn [acc]
                                               {:name acc,
                                                :infer-count (get acc-count
                                                                  acc),
                                                :infer-category category})
                                         candidate-account-names)]
                (vec (sort
                       ;; by infer-count descending, then by name ascending
                       (fn [acc0 acc1]
                         (let [count-cmp (compare (:infer-count acc1)
                                                  (:infer-count acc0))]
                           (if (not= count-cmp 0)
                             count-cmp
                             (compare (:name acc0) (:name acc1)))))
                       annotated-accounts))))
          secondary-accounts
            (cond found-payee (order-accounts found-payee "payee")
                  found-narration (order-accounts found-narration "narration")
                  (> units 0) [{:name "Income:Unknown"}]
                  (< units 0) [{:name "Expenses:Unknown"}]
                  :else [])]
      (assoc txn :acc2 secondary-accounts))))
