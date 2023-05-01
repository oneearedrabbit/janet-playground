A simple in-memory client-server scheduler in Janet running background jobs in threads.

```
janet worker.janet | tee tasks.jdn
janet client.janet  # run task1 from tasks.janet
```

Edit tasks in `tasks.janet`

I tested it with `ab` calling an `httpf` endpoint and requesting 50000 messages total, 1000 concurrent requests. The scheduler could process ~8k simple tasks a second on Macbook 2013, no messages lost. It could process (much) more if worker/client running without httpf, but ~8k was already good enough for me, and I didn't bother testing it further.

It is missing a function to load processed tasks from `tasks.jdn`. It is easy to add, if persistance is a requirement.
