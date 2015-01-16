This is a correctness test that attempts to do partitioned messaging and use state. It is meant to be run while killing samsa and kafka machines to test fault-tolerance.

It runs in iterations and each iteration has a correctness criteria that is checked before launching the next iteration. Here are the jobs and their function

emitter.samza:
  This job takes input from the "epoch" topic. Epochs are number 0, 1, 2,...
  For each new epoch each emitter task does something like the following:
     for i = 0...count:
       send("emitted", i, partition)
  where partition is the task partition id.
  
joiner.samza:
  This job takes in the emitted values from emitter and joins them together by key.
  When it has received an emitted value from each partition it outputs the key to the topic "completed".
  To track which partitions have emitted their value it keeps a store with | seperated numbers. 
  The first entry is the epoch and the remaining entries are partitions that have emitted the key.
  
checker.samza:
  This job has a single partition and stores all the completed keys. When all the keys are completed it sends an incremented epoch to the epoch topic, kicking off a new round.
  
watcher.samza:
  This job watches the epoch topic. If the epoch doesn't advance within some SLA this job sends an alert email.
  
The state maintained by some of these jobs is slightly complex because of the need to make everything idempotent. So, for example, instead of keeping the partition count
in the joiner job we keep the set of partitions so that double counting can not occur.

To run, simply start all four jobs at once.
