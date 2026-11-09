use evdev::{AttributeSet, Device, EventType, InputEvent, Key, uinput::VirtualDeviceBuilder};
use std::collections::{HashSet, VecDeque};
use std::env;
use std::sync::mpsc::{RecvTimeoutError, channel};
use std::thread;
use std::time::{Duration, Instant};

// This many new normal keys inside one time window counts as a burst.
const LIMIT: usize = 3;
// Every key event is held back this long so a burst can be spotted before it is forwarded.
const WINDOW: Duration = Duration::from_millis(10);

const MODS: [Key; 8] = [
    Key::KEY_LEFTCTRL,
    Key::KEY_RIGHTCTRL,
    Key::KEY_LEFTSHIFT,
    Key::KEY_RIGHTSHIFT,
    Key::KEY_LEFTALT,
    Key::KEY_RIGHTALT,
    Key::KEY_LEFTMETA,
    Key::KEY_RIGHTMETA,
];

fn is_mod(code: u16) -> bool {
    MODS.iter().any(|k| k.code() == code)
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

    loop {
        let timeout = match pending.front() {
            Some((_, t)) => WINDOW.saturating_sub(t.elapsed()),
            None => Duration::from_secs(3600),
        };

        match rx.recv_timeout(timeout) {
            Ok((ev, t)) => {
                let code = ev.code();
                let normal_down = ev.value() == 1 && !is_mod(code);

                if normal_down && t < burst_until {
                    // Late part of a burst that was already detected.
                    dropped.insert(code);
                } else {
                    pending.push_back((ev, t));

                    if normal_down {
                        let recent: Vec<u16> = pending
                            .iter()
                            .filter(|(e, pt)| {
                                e.value() == 1
                                    && !is_mod(e.code())
                                    && t.duration_since(*pt) <= WINDOW
                            })
                            .map(|(e, _)| e.code())
                            .collect();

                        if recent.len() >= LIMIT {
                            for c in recent {
                                dropped.insert(c);
                            }
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
