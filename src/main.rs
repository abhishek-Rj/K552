use evdev::{AttributeSet, Device, EventType, InputEvent, Key, uinput::VirtualDeviceBuilder};
use std::collections::{HashSet, VecDeque};
use std::env;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

// This many new key presses (modifiers included) inside one time window counts as a burst.
// Real typing almost never has this many keys go down within a few milliseconds.
const LIMIT: usize = 4;
// Every key event is held back this long so a burst can be spotted before it is forwarded.
// A burst can arrive in chunks about 10 ms apart, so this needs to be well above 10 ms.
const WINDOW: Duration = Duration::from_millis(30);

// The keys that start the bursts on this keyboard. When a burst happens and exactly one
// of these is inside it, that key is treated as the real press and kept.
const TRIGGERS: [Key; 4] = [Key::KEY_G, Key::KEY_T, Key::KEY_V, Key::KEY_SPACE];

fn is_trigger(code: u16) -> bool {
    TRIGGERS.iter().any(|k| k.code() == code)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths: Vec<String> = env::args().skip(1).collect();
    if paths.is_empty() {
        eprintln!("usage: sudo k552_filter /dev/input/eventA /dev/input/eventB ...");
        std::process::exit(1);
    }

    // Open every interface of the keyboard and collect the union of their keys.
    let mut devices = Vec::new();
    let mut all_keys = AttributeSet::<Key>::new();
    for p in &paths {
        let dev = Device::open(p)?;
        if let Some(keys) = dev.supported_keys() {
            for k in keys.iter() {
                all_keys.insert(k);
            }
        }
        devices.push(dev);
    }

    let mut vdev = VirtualDeviceBuilder::new()?
        .name("k552_filtered")
        .with_keys(&all_keys)?
        .build()?;

    // One reader thread per interface, all feeding the same channel.
    let (tx, rx) = channel::<(InputEvent, Instant)>();
    for mut dev in devices {
        dev.grab()?;
        let tx = tx.clone();
        thread::spawn(move || {
            loop {
                match dev.fetch_events() {
                    Ok(events) => {
                        for ev in events {
                            if ev.event_type() == EventType::KEY {
                                if tx.send((ev, Instant::now())).is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(_) => return,
                }
            }
        });
    }
    drop(tx);

    println!("filtering {} interfaces, press Ctrl+C to stop", paths.len());

    let mut pending: VecDeque<(InputEvent, Instant)> = VecDeque::new();
    // Keys whose press was dropped, so their release and repeats are dropped too.
    let mut dropped: HashSet<u16> = HashSet::new();
    let mut burst_until = Instant::now();
    // True once the current burst has already settled on a real trigger key.
    let mut burst_has_trigger = false;

    loop {
        let timeout = match pending.front() {
            Some((_, t)) => WINDOW.saturating_sub(t.elapsed()),
            None => Duration::from_secs(3600),
        };

        match rx.recv_timeout(timeout) {
            Ok((ev, t)) => {
                let code = ev.code();
                let is_down = ev.value() == 1;

                if is_down && t < burst_until {
                    // Late part of a burst that was already detected.
                    if is_trigger(code) && !burst_has_trigger {
                        burst_has_trigger = true;
                        pending.push_back((ev, t));
                    } else {
                        dropped.insert(code);
                    }
                } else {
                    pending.push_back((ev, t));

                    if is_down {
                        let recent: Vec<u16> = pending
                            .iter()
                            .filter(|(e, pt)| e.value() == 1 && t.duration_since(*pt) <= WINDOW)
                            .map(|(e, _)| e.code())
                            .collect();

                        if recent.len() >= LIMIT {
                            let triggers: Vec<u16> =
                                recent.iter().copied().filter(|c| is_trigger(*c)).collect();
                            let keep = if triggers.len() == 1 {
                                Some(triggers[0])
                            } else {
                                None
                            };

                            let names: Vec<Key> = recent.iter().map(|c| Key::new(*c)).collect();
                            eprintln!("burst: {:?} keeping {:?}", names, keep.map(Key::new));

                            for c in recent {
                                if Some(c) != keep {
                                    dropped.insert(c);
                                }
                            }
                            burst_has_trigger = !triggers.is_empty();
                            burst_until = t + WINDOW;
                        }
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }

        // Forward everything that has waited long enough and was not marked as junk.
        while let Some((_, t)) = pending.front() {
            if t.elapsed() < WINDOW {
                break;
            }
            let (ev, _) = pending.pop_front().unwrap();
            let code = ev.code();

            if dropped.contains(&code) {
                if ev.value() == 0 {
                    dropped.remove(&code);
                }
                continue;
            }
            // emit adds the closing SYN_REPORT by itself.
            vdev.emit(&[ev])?;
        }
    }

    Ok(())
}
