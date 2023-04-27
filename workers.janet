(import spork/json)
(import uuid)
(import circlet)
(import janet-html :as h)

(def total-workers 5)
(def supervisors @[])

(defn queue/length [q]
  (def stack1 (get q :stack1))
  (def stack2 (get q :stack2))
  (+ (length stack1) (length stack2)))
(defn queue/empty? [q]
  (= 0 (queue/length q)))
(defn queue/new [&opt size]
  (default size 100)
  @{:size size :stack1 @[] :stack2 @[]})
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

(defn nursery/new [&opt nursery-size]
  (default nursery-size 20)
  {:pending (queue/new)
   :capacity nursery-size
   :running @{}})
(defn nursery/schedule [job & args]
  (def pool (math/floor (* total-workers (math/random))))
  (def supervisor (pool supervisors))
  (def task (task/new job ;args supervisor))
  (ev/give supervisor [:enqueue task])
  task)
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
(defn nursery/worker [i chan]
  (def nursery (nursery/new))

  (loop [item :iterate (ev/take chan)]
    (def task
      (match item
        [:enqueue job] (nursery/enqueue nursery job)
        [:ok task-id] (do 
                        (print (json/encode (task/serialize (nursery/find nursery task-id))))
                        (nursery/release nursery task-id))
        [:error task-id err] (do
                               (eprint (json/encode (task/serialize (nursery/find nursery task-id))))
                               (nursery/release nursery task-id))))

    (when task
      (ev/thread (fiber/new (task :fn)) (task :task-id) :nt))))
(defn nursery/setup []
  (for i 0 total-workers
    (def chan (ev/thread-chan))
    (array/push supervisors chan)
    (ev/spawn-thread (nursery/worker i chan) nil :n)))

(defn test-worker [i]
  (print "start " i)
  (ev/sleep 1)
  (print "done " i))

(defmacro text/html-header []
  {"Content-Type" "text/html; charset=utf-8"})

(defn polling-partial []
  (def dummy-task (nursery/schedule test-worker "hello"))
  (dummy-task :task-id))

(defn home-path [req]
  {:status 200
   :headers (text/html-header)
   :body
     (h/encode
      [:html
         [:head
            [:script {:src "https://unpkg.com/htmx.org@1.9.0" }]]
       [:body
          [:p "Hi"]
        [:div {:hx-get "/polling" :hx-trigger "every 2s"} (polling-partial)]]])})

(defn coin/flip []
  (> (math/random) 0.5))

(defn polling-path [req]
  (if (coin/flip)
    {:status 200
     :headers (text/html-header)
     :body (polling-partial)}
    {:status 286
     :headers (text/html-header)
     :body (h/encode "Done!")}))

(defn main
  "Draw me a sheep!"
  [& args]

  (math/seedrandom (os/cryptorand 8))
  (nursery/setup)

  (circlet/server
   (->
    {"/" home-path
     "/polling" polling-path}
    circlet/router
    circlet/logger
    )
   8000))
