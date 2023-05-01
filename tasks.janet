(import http)
(import spork/json)
(import ./misc)

(defn task2 [a]
  (* 2 a)

(defn task1 [a b]
  (def double-a ((misc/worker-schedule-and-wait "task2" a) :output))
  (+ double-a b))
