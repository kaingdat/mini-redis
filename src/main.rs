use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                tokio::spawn(async move {
                    let mut buf = [0; 512];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(_) => {
                                let _ = stream.write_all(b"+PONG\r\n").await;
                            }
                            Err(e) => {
                                eprintln!("error: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}
