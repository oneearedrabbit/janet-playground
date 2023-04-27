(import spork/json)
(import uuid)

(math/seedrandom (os/cryptorand 8))

(def total-workers 1)
(def queue-size 10)
(def nursery-size 20)
(def total-elements 10)
(def supervisors @[])

(defn queue/length [q]
  (def stack1 (get q :stack1))
  (def stack2 (get q :stack2))
  (+ (length stack1) (length stack2)))
(defn queue/empty? [q]
  (= 0 (queue/length q)))
(defn queue/new [size] @{:size size :stack1 @[] :stack2 @[]})
(defn queue/push [q el] (array/push (get q :stack1) el))
(defn queue/pop [q]
  (def stack1 (get q :stack1))
  (def stack2 (get q :stack2))
  (when (= 0 (length stack2))
    (for i 0 (length stack1)
      (array/push stack2 (array/pop stack1))))
  (when (= 0 (length stack2))
    (error "Queue is empty."))
  (array/pop stack2))

(defn nursery/new []
  {:pending (queue/new queue-size)
   :capacity nursery-size
   :running @{}})
(defn nursery/enqueue [q el]
  (if (> (q :capacity) (length (q :running)))
    (set ((q :running) (el :task-id)) el)
    (do
      (queue/push (q :pending) el)
      nil)))
(defn nursery/find [q task-id]
  ((q :running) task-id))
(defn nursery/next [q]
  (when (not (queue/empty? (q :pending)))
    (nursery/enqueue q (queue/pop (q :pending)))))
(defn nursery/release [q task-id]
  (set ((q :running) task-id) nil)
  (nursery/next q))
(defn nursery/empty? [q] (queue/empty? (q :pending)))

(defn test-worker [i]
  (math/seedrandom (os/cryptorand 8))

  (print "start " i)
  (def delay (math/random))
  (ev/sleep delay)
  (print "done " i))

(defn task/new [task args chan]
  (def task-id (uuid/generate))
  @{:task-id task-id
    :time-queued (os/time)
    :fn (fn []
          (try
            (do
              (task args)
              (ev/give chan [:ok task-id]))
            ([err]
             (ev/give chan [:error task-id err]))))})

(defn task/serialize [task]
  @{:task-id (task :task-id)
    :time-queued (task :time-queued)})

(defn worker [i chan]
  (def nursery (nursery/new))

  (loop [item :iterate (ev/take chan)]
    (def task
      (match item
        [:enqueue task args] (nursery/enqueue nursery (task/new task args chan))
        [:ok task-id] (do 
                        (print (json/encode (task/serialize (nursery/find nursery task-id))))
                        (nursery/release nursery task-id))
        [:error task-id err] (do
                               (eprint (json/encode (task/serialize (nursery/find nursery task-id))))
                               (nursery/release nursery task-id))))

    (when task
      (ev/thread
       (fiber/new (task :fn)) (task :task-id) :nt))))

(for i 0 total-workers
  (def chan (ev/thread-chan))
  (array/push supervisors chan)
  (ev/spawn-thread (worker i chan) nil :n))

(for i 0 total-elements
  (def pool (math/floor (* total-workers (math/random))))
  (ev/give (pool supervisors) [:enqueue test-worker i]))
