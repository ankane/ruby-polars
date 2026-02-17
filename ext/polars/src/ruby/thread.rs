use std::any::Any;
use std::sync::OnceLock;
use std::sync::mpsc::{RecvTimeoutError, SyncSender, sync_channel};

use magnus::Ruby;
use magnus::error::RubyUnavailableError;

use crate::ruby::gvl::GvlExt;

type BackgroundMessage = (
    Box<dyn FnOnce(&Ruby) -> Box<dyn Any + Send> + Send>,
    SyncSender<Box<dyn Any + Send>>,
);

static BACKGROUND_THREAD_MAILBOX: OnceLock<SyncSender<BackgroundMessage>> = OnceLock::new();

// TODO figure out better approach
pub(crate) fn start_background_ruby_thread(rb: &Ruby) {
    BACKGROUND_THREAD_MAILBOX.get_or_init(|| {
        let (sender, receiver) = sync_channel::<BackgroundMessage>(0);

        // TODO save reference to thread?
        rb.thread_create_from_fn(move |rb2| {
            rb2.detach(|| {
                loop {
                    match receiver.recv_timeout(std::time::Duration::from_millis(10)) {
                        Ok((f, sender2)) => {
                            Ruby::attach(|rb3| sender2.send(f(rb3)).unwrap());
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            Ruby::attach(|rb3| rb3.thread_schedule());
                        }
                        Err(RecvTimeoutError::Disconnected) => {
                            todo!();
                        }
                    }
                }

                #[allow(unreachable_code)]
                Ok(())
            })
        });

        sender
    });
}

pub(crate) fn run_in_ruby_thread<T, F>(func: F) -> T
where
    T: Send + 'static,
    F: FnOnce(&Ruby) -> T + Send + 'static,
{
    let func2 = move |rb: &Ruby| -> Box<dyn Any + Send> { Box::new(func(rb)) };
    let (sender, receiver) = sync_channel(0);
    BACKGROUND_THREAD_MAILBOX
        .get()
        .unwrap()
        .send((Box::new(func2), sender))
        .unwrap();
    *receiver.recv().unwrap().downcast().unwrap()
}

pub(crate) fn is_non_ruby_thread() -> bool {
    matches!(Ruby::get(), Err(RubyUnavailableError::NonRubyThread))
}
