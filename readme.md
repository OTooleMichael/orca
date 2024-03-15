# YetAnotherOrchestrator

## Naming

***Orca***:
- Just the nicest name, long standing, already used to
- impossible to get domain. Hundreds of companies with that name preexisting. It's hard to google and end up with actual results pointing at this product.


***yetanotherX***:
- yalf / yaflow / yatt (yet another tranformer tool)


***openX***:
- open orchestrator? open task engine?



***Cordyna***:
- all tld domains still available
- -> would be easy to find when you google
- "dyna do x" -> fine cli command
- memorizable like a girl's name
- "coordination" / "core" + "dynamic"
- brandable / unique
- multi-purpose: non-data and non-product limited -> can also be used as company name for general contracting, plus binding in adjacent ppl's projects like e.g. marketing-alex, or pretty much anything else, should we have to pivot..


***APF*** / A Pipeline Framework
![alt text](image-1.png)


Other potential names for inspiration:
- chaincontrol.cc
- distribuworx
- enginex
- flowstron
- implink
- workfloz
- centrynk
- meshlynk
- orchex
- pipefy
- lucidata
- pyramesh
- meshmatic
- oakviz
- goflow
- syncord
- centraflow
- disflow
- meshvent
- dataflux
- taskflower
- centrall
- coordi
- coreflow
- corex
- conduit
- plixy
- cordex
- distribu
- anyflow
- centriflux
- hypergrid
- flowtronic
- luftask
- archit
- foxgorithm


## Mission:
- Centrally manage distributed workloads

- Orchestrate any set of tasks. Language agnostic and non-prohibitive in objective. "A task" can be anything, a data-import job in rust, an e-mail distribution in java, a google sheet update in python, an SQL-transformation pipeline with dbt, you name it. A task may depend on other tasks in the network to have run first, or to have run within a certain timeframe. These "upstream-dependencies" may be owned and maintained by a different team and may be executed on a different server.

- Dynamically assessing the required run-order.

- Enforcing compliance with standardized minimalistic communication framework for all participants in the network. As such, ensuring continuous effective collaboration among multiple inter-dependendent dev-teams.

- While being a joint effort of engineers and analysts with backgrounds in various analytical data processing setups, the project aims to also be applicable to any other problem-field with complex task-based orchestration requirements

- In the absence of any presently available tooling that sufficiently supports the data pipelining efforts of medium to large-scale enterprises, we provide a solution that aims towards granting self-reliable provisioning-capabilities to data-originating, data-processing, and data-consuming teams. As such, it can be seen as a foundational framework that support the implementation of a 🌈💥✨***data-mesh***✨💥🌈

- The minimalistic framework approach guarantees ease of use and a broad applicability to task-based orchestration problems, rather than creating the need to suppress or circumvent certain unwanted behaviours of more opinionated / purpose-tailored tools on the market, in order to fit them to the individual requirements.

- The framework targets to mitigate the common issue of seemingly disconnected code changes of one team, breaking the "down-stream" code of another team. That is achieved by providing the opportunity of integrated testing, across all the teams that are involved along the data pipeline [generation to dashboard], as opposed to having the teams test their code in isolation. The framework supports with correctly identifying and notififying the responsible team if issues occur.

- The framework is language-agnostic. While a standard task-server in python is provided, anyone is invited to write their own implementation of a task-server in any language, that can be custom tailored to perform any set of tasks. The only requirement is the compliance with the event-based communication protocol to interact with the central "cerebro" instance.



## How Run?
- get a docker engine (we advise for orbstack)
- install docker-compose
- clone the repo
- run ```docker-compose up``` in the project's root-dir
- you should see 4 services booting:
    - cerebro
    - redis
    - task-server-a
    - task-server-b
- (yet to come: "[name] run task-3" cli commands )



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


## up next
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
- basic github repo control (only squash merge, (proper tick number), mandatory reviewers, auto-close branch post-merge, repo/org-wide secrets for ci/cd
- pre-commit linting



## What if ..?
- what if cerebro is down? recoverability? missed crons?
- what if a task-server ran half of the tasks and then reboots?
- what if we behaved to the outside like a normal API? i.e. you can command / request from the outside like with any other api, but the eventbased comms are limited to the task-server-cerebro comms?
