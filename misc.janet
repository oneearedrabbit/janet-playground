(import jdn)

(defn read-by-line [chunks]
  (var extra "")
  (coro
   (loop [chunk :in chunks]
     (def lines (string/split "\r\n" (string extra chunk)))
     (set extra (array/pop lines))
     (loop [line :in lines]
       (yield (string/trim line))))))

(defn reader [conn]
  (generate [chunk :iterate (:read conn 1024)] chunk))

(defn worker-request [& args]
  (let [conn (net/connect "127.0.0.1" "8001")]
    (defer (:close conn)
      (:write conn (string (jdn/encode args) "\r\n"))
      (resume (read-by-line (reader conn))))))
(defn worker-schedule [task & args]
  (def resp (worker-request :e task ;args))
  (match (string/split " " resp)
    ["OK" task-id] task-id
    ["ERR"] (pp "ERR")))
(defn worker-find [task-id]
  (def resp (worker-request :f task-id))

  (match (string/split " " resp)
    ["OK"] (jdn/decode (string/slice resp 3 nil))
    ["ERR"] (pp "ERR")))
(defn worker-schedule-and-wait [job & args]
  (def task-id (worker-schedule job ;args))
  (var task nil)
  (while true
    (if-let [running-task (worker-find task-id)]
      (when (= :finished (running-task :status))
        (and (set task running-task) (break))))
    (ev/sleep 0.3))
  task)
