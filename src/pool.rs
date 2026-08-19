//! pool — ONE set of worker threads for the whole library.
//!
//! Every parallel phase used to spawn its own: 48 threads with 8 MiB
//! stacks per `pq_read`, per `pq_write`, per footer open.  A streaming
//! caller that walks a file row group by row group pays that on every
//! call — measured at 10-20 ms of a wide read, and it is pure overhead.
//! The pool creates its threads once, lazily, and parks them on a
//! condvar in between.
//!
//! The contract is deliberately narrow: `run(n, f, me)` runs `f` on `n`
//! pool workers while the CALLING thread runs `me`, and returns when
//! all of them are done.  That is all any caller here needs — the work
//! itself is claimed from an atomic cursor inside `f` — and it is what
//! lets the job be borrowed rather than boxed and moved.
//!
//! Two situations fall back to spawning, and both must:
//!   * the pool is already running a job (a nested call, or two host
//!     threads in the library at once) — one job at a time is what
//!     makes the borrowed-closure dispatch sound;
//!   * the process FORKED since the pool was built (`peach`), so the
//!     threads the child thinks it has do not exist.  The pid check is
//!     first, before any lock the fork may have copied in a held state.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Condvar, Mutex, OnceLock};

/// Worker count for every parallel phase in this library.  It lives
/// here because the pool sizes itself from it and every other caller
/// asks the pool for workers.
pub fn nthreads() -> usize {
    std::thread::available_parallelism().map_or(1, |v| v.get())
}

/// The job every worker of a run executes.  A raw pointer because it is
/// BORROWED: `run` blocks until every worker has finished with it, so
/// it cannot outlive the frame that lent it.
#[derive(Clone, Copy)]
struct Job(*const (dyn Fn() + Sync));
unsafe impl Send for Job {}
unsafe impl Sync for Job {}

#[derive(Default)]
struct State {
    job: Option<Job>,
    /// Bumped once per dispatch; a worker runs when it changes.
    gen: u64,
    /// How many of the pool's workers this job wants.
    want: usize,
    /// How many have still to finish it.
    left: usize,
    /// How many died in it.
    panics: usize,
}

struct Pool {
    st: Mutex<State>,
    work: Condvar,
    done: Condvar,
    /// One job at a time (see the module note).
    busy: Mutex<()>,
    /// Threads the pool will have.
    n: usize,
    /// The process that created them.
    pid: u32,
    /// Workers parked so far — bumped by each one as it STARTS, so it
    /// climbs while the builder is still spawning, and `run` falls back
    /// to fresh threads until the count covers its ask: building 48
    /// workers costs 2 ms of `clone(2)` but 60-190 ms of first-touch
    /// (each one's allocator arena) — measured, and it is exactly the
    /// latency a one-shot process would never earn back.  So the FIRST
    /// call in a process pays what it always paid, and the pool is
    /// there for the second.
    ready: AtomicUsize,
    /// Guards the one-time build.
    started: OnceLock<()>,
}

static POOL: OnceLock<Pool> = OnceLock::new();

/// Build the pool: parked workers that live for the process.
fn build() -> Pool {
    Pool {
        st: Mutex::new(State::default()),
        work: Condvar::new(),
        done: Condvar::new(),
        busy: Mutex::new(()),
        n: nthreads(),
        pid: std::process::id(),
        ready: AtomicUsize::new(0),
        started: OnceLock::new(),
    }
}

/// Start building the workers, once, WITHOUT waiting for them.
fn warm(p: &'static Pool) {
    p.started.get_or_init(|| {
        std::thread::Builder::new()
            .stack_size(1 << 16)
            .spawn(move || {
                for i in 0..p.n {
                    if std::thread::Builder::new()
                        .stack_size(crate::WORKER_STACK)
                        .spawn(move || serve(p, i))
                        .is_err()
                    {
                        return;                                                 // the pool is simply smaller
                    }
                }
            })
            .ok();
    });
}

/// Park loop of one worker.  `idx` decides whether a given job wants it.
fn serve(p: &'static Pool, idx: usize) {
    let mut seen = 0u64;
    // Counted as soon as this thread is alive: the cost a pooled call
    // still pays once is the first TOUCH of these 8 MiB stacks by real
    // decode frames, which no amount of pre-warming here avoids (tried:
    // pre-faulting an arena moved nothing) — and which the per-call
    // spawn it replaces used to pay on EVERY call.
    p.ready.fetch_add(1, Relaxed);
    loop {
        let job = {
            let mut s = p.st.lock().unwrap_or_else(|e| e.into_inner());
            while s.gen == seen {
                s = p.work.wait(s).unwrap_or_else(|e| e.into_inner());
            }
            seen = s.gen;
            (idx < s.want).then_some(s.job).flatten()
        };
        let Some(j) = job else { continue };
        // A panic must not take the worker thread down with it: the
        // caller is waiting on a count that only this thread decrements.
        let r = catch_unwind(AssertUnwindSafe(|| unsafe { (*j.0)() }));         //
        let bad = r.is_err();                                                   //
        let mut s = p.st.lock().unwrap_or_else(|e| e.into_inner());
        s.panics += bad as usize;
        s.left -= 1;
        if s.left == 0 {
            p.done.notify_all();
        }
    }
}

/// Run `f` on `n` workers while this thread runs `me`; returns `me`'s
/// value and whether any worker panicked.  `n` above the pool's size is
/// clamped: asking for more workers than the machine has is not an
/// error, it is just the same work in more rounds.
pub fn run<R>(
    n: usize,
    f: &(dyn Fn() + Sync),
    me: impl FnOnce() -> R,
) -> (R, bool) {
    if n == 0 {
        return (me(), false);
    }
    let p = POOL.get_or_init(build);
    // A forked child (peach) inherited the bookkeeping but none of the
    // threads, so the pid check comes FIRST, before any lock the fork
    // may have copied in a held state.
    let up = p.pid == std::process::id() && p.ready.load(Relaxed) >= n;
    let held = up.then(|| p.busy.try_lock().ok());
    let Some(Some(_held)) = held else {
        // Fresh threads for this call, and only THEN start building the
        // pool: a short call would otherwise spend its own cores on 48
        // thread starts it cannot benefit from (measured: a 50 ms
        // 2-column read paid 50 ms extra for the overlap).
        let r = spawned(n, f, me);
        warm(p);
        return r;
    };
    let n = n.min(p.n);
    {
        let mut s = p.st.lock().unwrap_or_else(|e| e.into_inner());
        // Erase the borrow: sound because the wait below returns only
        // after every worker is finished with the pointer.
        s.job = Some(Job(unsafe {
            std::mem::transmute::<
                *const (dyn Fn() + Sync + '_),
                *const (dyn Fn() + Sync + 'static),
            >(f as *const _)
        }));
        s.want = n;
        s.left = n;
        s.panics = 0;
        s.gen += 1;
        p.work.notify_all();
    }
    // `me` may unwind; the workers must still be waited for first, or
    // the job pointer they hold outlives the frame that lent it.
    let r = catch_unwind(AssertUnwindSafe(me));
    let bad = {
        let mut s = p.st.lock().unwrap_or_else(|e| e.into_inner());
        while s.left > 0 {
            s = p.done.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        s.job = None;
        s.panics > 0
    };
    match r {
        Ok(v) => (v, bad),
        Err(e) => resume_unwind(e),
    }
}

/// Run `f(0)..f(n-1)` across the pool and collect the answers IN
/// ORDER, or None when a worker panicked.  One shared cursor rather
/// than a static split: the items differ wildly in cost — a footer of
/// three row groups against one of a thousand, a 2000-entry dictionary
/// page against an empty one — so the work is handed out as it is
/// finished.  Every slot is written exactly once, by whichever worker
/// claimed that index.
pub fn par_map<T: Send>(
    n: usize,
    f: impl Fn(usize) -> T + Sync,
) -> Option<Vec<T>> {
    let out: Vec<Mutex<Option<T>>> =
        (0..n).map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    let body = || loop {
        let i = next.fetch_add(1, Relaxed);
        if i >= n {
            return;
        }
        *out[i].lock().unwrap_or_else(|p| p.into_inner()) = Some(f(i));
    };
    let (_, panicked) =
        run(nthreads().min(n).saturating_sub(1), &body, body);
    if panicked {
        return None;
    }
    out.into_iter()
        .map(|m| m.into_inner().unwrap_or_else(|p| p.into_inner()))
        .collect()
}

/// The fallback: fresh scoped threads, same contract.
fn spawned<R>(
    n: usize,
    f: &(dyn Fn() + Sync),
    me: impl FnOnce() -> R,
) -> (R, bool) {
    let bad = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n)
            .map(|_| {
                std::thread::Builder::new()
                    .stack_size(crate::WORKER_STACK)
                    .spawn_scoped(s, f)
                    .expect("pq: spawn")
            })
            .collect();
        let r = catch_unwind(AssertUnwindSafe(me));
        for h in hs {
            if h.join().is_err() {
                bad.fetch_add(1, Relaxed);
            }
        }
        match r {
            Ok(v) => (v, bad.load(Relaxed) > 0),
            Err(e) => resume_unwind(e),
        }
    })
}
