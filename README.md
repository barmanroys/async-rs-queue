### Asynchronous Task Queue in Rust

Essentially, a Rust based re-implementation of
my [previous project](https://github.com/barmanroys/async-coroutine/blob/master/README.md) written in Python. To
appreciate this project, I highly recommend you going through the previous one's README to get the full context.

### Motivation

As previously established, [TaskIQ](https://taskiq-python.github.io/) provides an excellent loosely coupled generic task
dispatch framework in python without the producers being aware of the exact task definition. However, the library uses
very python specific transport protocol to communicate with the broker (Redis, in our example) which makes it unsuitable
for a multi-lingual set up.

As I was looking for similar functionalities in Rust, and I could not find a readily available library, I decided to
build the basic primitives myself, not necessarily for a production grade system, but at the very least as a learning
experience and for deeper insights into the transport protocol.

### Task Data Flow

                ┌──────────────┐
                │   Producer   │
                │              │
                │ create task  │
                │ serialize   │
                └──────┬───────┘
                       │
                       ▼
                 ┌───────────┐
                 │   Redis   │
                 │   Broker  │
                 └─────┬─────┘
                       │
                 blocking wait
                       │
                       ▼
                ┌──────────────┐
                │    Worker    │
                │              │
                │ consume task │
                │ execute      │
                │ store result │
                └──────────────┘

The producer submits a serialised `BrokerMessage` to Redis. The worker blocks waiting for work, parses the message,
executes the task, and stores the result back in Redis. The client can subsequently retrieve the result using the task
ID.

#### The Interesting Bits

The actual task queuing mechanism is deliberately boring.

* Redis dequeue for task input storage
* `LPUSH` to enqueue work (used by producer)
* `BRPOP` for consumers to atomically dequeue tasks
* `SET` for consumers to write back result
* `GET` for producers/clients to read or check for the presence of output

The interesting part is not that Redis can do it (it obviously can), but to establish how little infrastructure and code
does it take to get from

```
submit_task
```

to

```
worker.wait_for_task()
```

to

```
get_result(task_id)
```

### Transport Protocol

The small messaging protocol dictates what gets written to the Redis and how the clients/workers interpret them. The
protocol is intentionally defined independently of the producer and worker implementations. A producer only needs to
agree on the serialized message format with the worker; neither side needs to know the other's task implementation.

```Rust
pub enum BrokerMessage { // A small abstraction over input and output
    Input(TaskInputMessage),
    Output(TaskOutputMessage),
}
```

and

```Rust
pub enum Value { // Another abstraction to show different types of kwargs arguments
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Bytes(Vec<u8>),
}
```

Nothing here is intended to compete with production task queues. The point is to expose the plumbing that production
systems inevitably have to deal with.

#### Choice of Language

Apart from the performance, compiler safety guarantee and ownership enforcement, I also wanted to explore the limits of
Rust eco-system in terms of how much is handed over as ready-made solution (like I came to expect as a Python guy) vs
how much are you supposed to DIY. Some of the interesting Rust aspects this project deals with are

- ownership and borrowing
- traits and trait implementations
- `Result` / `Option`
- async Rust and Tokio
- serialization with `serde`
- module and crate organization
- multiple Cargo binaries
- asynchronous Redis clients
- explicit data modelling with enums

Coming from a Python-heavy data/ML engineering background, Rust makes some of the assumptions that Python lets you
ignore suddenly become compiler-enforced architectural decisions.

Which is annoying.

Also extremely instructive, to the point of being addictive once you let it grow on you.

#### Run It

Dependencies

* [Docker compose](https://docs.docker.com/compose/) to fire up a local redis instance. Feel free to use any other redis
  instance if available, and update the `REDIS_URL` in `.env`
* [Cargo](https://docs.rs/cargo/latest/cargo/), obviously

Fire up the Redis service locally by

```
docker compose up
```

Then, in another terminal (from the project root)

```bash
cargo run --bin consumer # Fire up the worker process
```

The worker will sit patiently waiting for work, rather than burning CPU in a polling loop. Because apparently sometimes
the most exciting thing a program can do is nothing until Redis tells it to wake up. In yet another terminal, from the
project root

```bash
cargo run --bin producer # Send a task and wait for the result
```

#### Redis Key Population

The `QUEUE_NAME` environment variable is the task input queue where producers write the tasks and consumers pick up. The
`RESULT_BACKEND` environment variable serves as a common prefix for all result keys. In case you are curious to see the
impact on Redis, use [Redis Insight](https://redis.io/insight/) to inspect the keys, which will reveal some of the
internal details.

### Limitation

This is intentionally a small PoC level project rather than a production-ready distributed task system. It does not
attempt to solve all the hard problems that mature task queues solve, such as:

* durable delivery guarantees
* retries and backoff
* task acknowledgement
* visibility
* timeouts
* worker heartbeats
* dead-letter
* scheduling
* distributed locking
* result expiration
* exactly-once semantics

Those are precisely the rabbit holes that make task queues interesting. The goal here was narrower:

> Understand the primitives before hiding them behind an abstraction.

If you're interested in the implementation, the code is intentionally small enough that the entire thing can be read
without needing a large pot of coffee.

And in case you have brewed that coffee anyway, here are some ideas (for enhancements) on how you can _really_
internalise the concepts and train the right muscles.

* Deploy multiple worker replicas under Kubernetes and explore horizontal scale-out and worker failure/recovery.
* Introduce retry semantics and heartbeats
* Failure and failed message logging, so that the client does not keep waiting forever.

As it is, the current implementation caters to the happy path where things go as planned, but in production they rarely
do. So if you can take on the challenge to make the system more resilient, feel free to create a pull request, or drop
me a line at my
[inbox](mailto:swagatopablo@aol.com).