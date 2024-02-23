# YetAnotherProjectWithoutName

i (thomas) have a clear preference:

![cordyna](image.png)

- all tld domains still available
- -> would be easy to find when you google
- "dyna do x" -> fine cli command
- memorizable like a girl's name
- "coordination" / "core" + "dynamic"
- brandable / unique
- multi-purpose: non-data and non-product limited (can also be used as company name for general contracting, plus binding in adjacent ppl's projects like e.g. marketing-alex, or pretty much anything else, should we have to pivot..


## Mission:
- create a tool that let's you orchestrate whatever (really) you want as freely as possible while enforcing maintenance of minimal formal practices that are necessary to efficiently collaborate across a large-scale inter-dependendent softare company (or an SME-company with multiple dev teams)

- this project is a collaboration of multiple engineers and analysts from the data and analytics industry, but aims to be applicable to any field with complex task-based orchestration requirements

- in the absence of any present sufficient tooling to ideally support the data pipelining efforts of medium to large-scale companies we provide a solution that aims towards building self-reliable source-data provisioning capabilities of data-originating teams within a company, and as such, we provide a foundational framework that, at its core, was built to support the acutal implementation of a 👋👋 data-mesh 👋👋




## Mission Plan:
create a clear distinction for test cases:

- cerebro server
- task server a (with independent workloads)
- task server b (with dependent workloads on server-a)


continuously test:
- graphbuilding capabilities of cerebro
- sends invocation-commands (events) in correct order to correct task-servers
- upon failure, still invokes anything that's not blocked by the failure
- cerebro waits for servers to report back on task states in various intervals
    - upon invocation command: 10 sec for "acknowledged" / "task-started"
    - upon "acknowledged": 10 min for "task-started"
    - upon "task-started": 30 min for "task-completed"


(there's more written down but it's too late to paste/refrac it all tonight)


## Vocab/Lingo
- state updates vs commands
- task state vs server state

- *TASK*: COMMAND["run"] vs COMMAND["invoke"]:

     cerebro '*invokes*': it sends an event that tells a task-server to '*run*' a task

- TASK_STATE["pending"]: cerebro has built the graph and is wating for upstream tasks to complete, before sending out a command to execute this particular task 
- TASK_STATE["queued"]: a task-server has received a run-command from cerebro to run a task, but it's currently busy with somehting else / has not yet actually started running the task
- TASK_STATE["started"]: a task-server has received a run-command from cerebro to run a task, and has now started running it
- TASK_STATE["completed"]: a task-server has received a run-command from cerebro to run a task, has already reported that it has TASK_STATE["started"], and now reports TASK_STATE["completed"] after having finished the task
TASK_STATE["error"]: there was either an error while actually processing the task OR cerebro created this event because no acknowledgement (queued/started) was given by the server within 10 sec. In the first case, the task-server's team is responsible for providing a proper description into the event

## what my goals are now (thomas)
- split the servers and make it clear who comms w/ whom
- make the task-servers declare themselvers to cerebro
- make it easy to cronjob a task while declaring it (e.g. 60 sec for test)
- make it possible to ad-hoc invoke a task from outside via event
- make it better observable what exactly happens in what server and when / in what order
- split the core tooling library from the actual task-specific stuff
- split the core tooling library from cerebro
- properly package the core tooling library
- (take it to the cloud once the test cases work)
- minimise the common dependencies in req.txt, just install ***insert-cool-name*** (ours)
- found a github organization under which we can create proper repos & access rights
- setup a minimal continuous cloud project + CI / CD
- prevent the docker mess (volume/build-cache/images/containers) after mulit-re-building the project locally