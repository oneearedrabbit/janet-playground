(import uuid)
(import jdn)
(import ./misc)

(defn queue/length [q]
  (def stack1 (get q :stack1))
  (def stack2 (get q :stack2))
  (+ (length stack1) (length stack2)))
(defn queue/empty? [q]
  (= 0 (queue/length q)))
(defn queue/new []
  @{:stack1 @[] :stack2 @[]})
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

(var nursery/in nil)
(var nursery/out nil)
(def all-tasks (dofile "src/tasks.janet")) # todo: this could be a module?
(defn task/new [task & args]
  (def task-id (uuid/generate))
  (def func ((get all-tasks (symbol task)) :value))
  @{:task-id task-id
    :time-queued (os/time)
    :name task
    :args args
    :status :new
    :fn (fn []
          (try
            (ev/give nursery/in [:ok task-id (func ;args)])
            ([err]
             (ev/give nursery/in [:error task-id err]))))})
(defn task/serialize [task]
  @{:task-id (task :task-id)
    :time-queued (task :time-queued)
    :name (task :name)
    :args (task :args)
    :status (task :status)
    :output (task :output)
    :error (task :error)})

(defn nursery/register [q task]
  (set ((q :registry) (task :task-id)) (task/serialize task)))
(defn nursery/enqueue [q el]
  (if (> (q :capacity) (length (q :running)))
    (do
      (set (el :status) :running)
      (nursery/register q el)
      (set ((q :running) (el :task-id)) el))
    (do
      (set (el :status) :pending)
      (nursery/register q el)
      (queue/push (q :pending) el)
      nil)))
(defn nursery/schedule [job & args]
  (def task (task/new job ;args))
  (ev/give nursery/in [:enqueue task])
  task)
# todo: this only searches for running tasks, needs a better name!
(defn nursery/find [q task-id]
  ((q :running) task-id))
(defn nursery/next [q]
  (when (not (queue/empty? (q :pending)))
    (nursery/enqueue q (queue/pop (q :pending)))))
(defn nursery/release [q task-id]
  (set ((q :running) task-id) nil)
  (nursery/next q))
(defn nursery/empty? [q] (queue/empty? (q :pending)))
(defn nursery/new [&opt nursery-size]
  (default nursery-size 50)
  {:pending (queue/new)
   :capacity nursery-size
   :running @{}
   :registry @{}})
(defn nursery/worker []
  (def nursery (nursery/new))

  (loop [item :iterate (ev/take nursery/in)]
    (match item
      [:find task-id] (do
                        (def task ((nursery :registry) task-id))
                        (if task
                          (ev/give nursery/out (task/serialize task))
                          (ev/give nursery/out nil)))
      [:enqueue job] (if-let [task (nursery/enqueue nursery job)]
                       (ev/thread (fiber/new (task :fn)) (task :task-id) :nt))
      [:ok task-id val] (do
                          (def task (nursery/find nursery task-id))
                          (set (task :status) :finished)
                          (set (task :output) val)
                          (nursery/register nursery task)
                          (file/write stdout (jdn/encode (task/serialize task)) "\n")
                          (file/flush stdout)
                          (nursery/register nursery task)
                          (if-let [task (nursery/release nursery task-id)]
                            (ev/thread (fiber/new (task :fn)) (task :task-id) :nt)))
      [:error task-id err] (do
                             (def task (nursery/find nursery task-id))
                             (set (task :status) :failed)
                             (set (task :error) err)
                             (nursery/register nursery task)
                             (file/write stderr (jdn/encode (task/serialize task)) "\n")
                             (file/flush stderr)
                             (nursery/register nursery task)
                             (if-let [task (nursery/release nursery task-id)]
                             (ev/thread (fiber/new (task :fn)) (task :task-id) :nt))))))

(defn main
  "Tasker"
  [& args]

  (set nursery/in (ev/thread-chan 1))
  (set nursery/out (ev/thread-chan 1))
  (ev/spawn-thread (nursery/worker) nil :n)

  (defn handler
    "Handle a connection in a separate fiber."
    [conn]
    (defer (:close conn)
      (loop [jdn :in (misc/read-by-line (misc/reader conn))]
        (try
            (match (jdn/decode jdn)
              [:e task & args] (do
                               (def task (nursery/schedule task ;args))
                               (net/write conn (string "OK " (task :task-id) "\r\n")))
              [:f task-id] (do
                             (ev/give nursery/in [:find task-id])
                             (def task (ev/take nursery/out))
                             (net/write conn (string "OK " (jdn/encode task) "\r\n"))))
          ([err]
           (net/write conn "ERR\r\n")
           (file/write stderr (jdn/encode {:error err}) "\n")
           (file/flush stderr))))))

  (ev/thread (fn []
               (let [server (net/listen "127.0.0.1" "8001")]
                 (forever
                  (if-let [conn (:accept server)]
                    (ev/call handler conn))))) nil :n))
