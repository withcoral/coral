//! A local listener for the production route constructor, without app bootstrap.
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;

pub(super) struct TestServer {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl TestServer {
    pub(super) async fn start(routes: tonic::service::Routes) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_routes(routes)
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    drop(stopped.await);
                })
                .await
        });
        Self {
            endpoint,
            shutdown: Some(shutdown),
            task,
        }
    }

    /// A channel that connects on first use, so accessors need no `await`.
    pub(super) fn channel(&self) -> tonic::transport::Channel {
        tonic::transport::Endpoint::from_shared(self.endpoint.clone())
            .unwrap()
            .connect_lazy()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        drop(self.shutdown.take());
        self.task.abort();
    }
}
