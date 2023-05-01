A simple in-memory client-server scheduler in Janet running background jobs in threads.

```
janet worker.janet | tee tasks.jdn
janet client.janet  # run task1 from tasks.janet
```

Edit tasks in `tasks.janet`

I tested it with `httpf`, and the scheduler could process ~8k messages a second on Macbook 2013, no messages lost. It could process more if running worker/client without httpf, but ~8k was already good enough for me, and I didn't bother testing it further.

It is missing a function to load processes tasks from `tasks.jdn`.
