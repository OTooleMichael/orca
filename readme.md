# YetAnotherProjectWithoutName


i (thomas) have obviously a clear preference 😄 :


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

- in the absence of any presently sufficient tooling to ideally support the data pipelining efforts of medium to large-scale enterprises, we provide a solution that aims towards building self-reliable provisioning-capabilities to data-originating teams. As such, we provide the first foundational framework that, at its core, was built to support the acutal implementation of a 👋👋 data-mesh 👋👋

- This framework prevents the common problem of seemingly disconnected code changes of one team, breaking downstream code of another team, as it aims to provide truly integrated testing across the whole data pipeline [generation to dashboard], and correctly identifies and notifies the responsible team if problems occur.

- We're language-agnostic. Whatever you want to run as an orchestrated task in a pipeline, you can run it. We provide a standard task-server project in python. You can easily build your own in the language of your preference, and we encourage / actively support those efforts.

- While built from a data-pipelining perspective, it's not limited to that at all. Any process can run as a task in our framework.




## How Run?
- (get a mac)
- get warp
- get orbstack
- clone repo
- docker-compose up
- (yet to come:) xxx run task-3
- you should see task-server-b, which hosts task-3 to ask cerebro to run it.
- this should trigger cerebro to run the upstream dependencies in correct order. Those taskss partially being managed by a different server (task-server-a).


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
- state updates vs commands (both via events)
- task state vs server state ("hey i'm up and have these tasks with these props" VS "hey i'm now running {This}")

- A task-server may boot and declare itself as SERVER_STATE["ready"]
    - right afterwards or as part of that ready-message, the server must declar what tasks it is responsible for.

- COMMAND["run"] vs COMMAND["invoke"]:

     cerebro '*invokes*': it sends an event that tells a task-server to '*run*' a task

- TASK_STATE["pending"]: cerebro has built the graph and is wating for upstream tasks to complete, before sending out a command to execute this particular task 
- TASK_STATE["queued"]: a task-server has received an invocation-command from cerebro to run a task, but it's currently busy with somehting else / has not yet actually started running the task
- TASK_STATE["started"]: a task-server has received an invocation-command from cerebro to run a task, and has now started running it
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



## What if ..?
- what if cerebro is down? recoverability? missed crons?
- what if a task-server ran half of the tasks and then reboots?
- what if we behaved to the outside like a normal API? i.e. you can command / request from the outside like with any other api, but the eventbased comms are limited to the task-server-cerebro comms?