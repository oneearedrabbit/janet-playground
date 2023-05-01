(import ./misc)

(def task-id (misc/worker-schedule "task1" (data "q")))
(def task (misc/worker-find task-id))
